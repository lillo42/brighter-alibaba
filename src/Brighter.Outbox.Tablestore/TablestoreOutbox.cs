using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Mime;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Aliyun.OTS;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search;
using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.DataModel.Search.Sort;
using Aliyun.OTS.Request;
using Brighter.Tablestore;
using Paramore.Brighter;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.Observability;

namespace Brighter.Outbox.Tablestore;

/// <summary>
/// Provides a synchronous and asynchronous outbox implementation for the Brighter command processor using Alibaba Cloud Table Store
/// as the backing storage. This implementation supports the outbox pattern for reliable message dispatching with at-least-once delivery semantics.
/// </summary>
/// <remarks>
/// <para>
/// This class implements both <see cref="IAmAnOutboxSync{TMessage, TTransaction}"/> and <see cref="IAmAnOutboxAsync{TMessage, TTransaction}"/>
/// interfaces, providing full support for synchronous and asynchronous operations required by the Brighter outbox pattern.
/// </para>
/// <para>
/// <strong>Key Implementation Details:</strong>
/// <list type="bullet">
/// <item><description>Uses Alibaba Cloud Table Store (OTS) as the persistent storage layer</description></item>
/// <item><description>Implements <see cref="NoTransaction"/> as Table Store doesn't support traditional distributed transactions</description></item>
/// <item><description>Provides idempotent operations through conditional writes and duplicate detection</description></item>
/// <item><description>Supports both synchronous and asynchronous method calls for flexibility</description></item>
/// <item><description>Includes comprehensive tracing and instrumentation support</description></item>
/// </list>
/// </para>
/// <para>
/// <strong>Table Schema Requirements:</strong>
/// The Table Store table must have the following structure:
/// <list type="bullet">
/// <item><description>Primary Key: "Id" (string) - Unique message identifier</description></item>
/// <item><description>Attribute: "DispatchedAt" (long) - Unix timestamp when dispatched (-1 for not dispatched)</description></item>
/// <item><description>Secondary Index: Required for efficient querying of dispatched/outstanding messages</description></item>
/// <item><description>Additional attributes for all message header properties and body content</description></item>
/// </list>
/// </para>
/// <para>
/// <strong>Error Handling:</strong>
/// The implementation gracefully handles duplicate insert attempts by ignoring OTSConditionCheckFail errors,
/// ensuring idempotent message additions to the outbox.
/// </para>
/// <para>
/// <strong>Performance Considerations:</strong>
/// Batch operations are implemented using Table Store's native batch APIs where available. For operations that don't have
/// native batch support, individual operations are executed sequentially with proper error handling.
/// </para>
/// </remarks>
/// <seealso cref="IAmAnOutboxSync{TMessage, TTransaction}"/>
/// <seealso cref="IAmAnOutboxAsync{TMessage, TTransaction}"/>
/// <seealso cref="TablestoreConfiguration"/>
/// <seealso cref="IAmATablestoreConnectionProvider"/>
public class TablestoreOutbox : IAmAnOutboxSync<Message, NoTransaction>, IAmAnOutboxAsync<Message, NoTransaction>
{
    /// <summary>
    /// Error code returned by Alibaba Cloud Table Store when a conditional operation fails (e.g., duplicate insert attempt).
    /// </summary>
    /// <remarks>
    /// This constant is used to identify and gracefully handle duplicate message insertion attempts,
    /// which is a common scenario in distributed systems with at-least-once delivery semantics.
    /// </remarks>
    private const string ErrorCodeConditionCheckFail = "OTSConditionCheckFail";
    
    /// <summary>
    /// Column name used to track the dispatch status of messages in Table Store.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This column stores a Unix timestamp in milliseconds indicating when the message was dispatched.
    /// A value of <c>-1</c> indicates that the message has not been dispatched yet.
    /// </para>
    /// <para>
    /// This column is indexed to enable efficient querying of dispatched and outstanding messages.
    /// </para>
    /// </remarks>
    private const string DispatchedAt = "DispatchedAt";
    
    private readonly IAmATablestoreConnectionProvider _connectionProvider;
    private readonly TablestoreConfiguration _configuration;
    private readonly TablestoreTable _table;
    
    /// <summary>
    /// Initializes a new instance of the <see cref="TablestoreOutbox"/> class with the specified configuration,
    /// creating a new connection provider internally.
    /// </summary>
    /// <param name="configuration">The configuration settings for the Table Store outbox.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when the outbox table name is not configured or is empty.</exception>
    /// <remarks>
    /// <para>
    /// This constructor creates a new <see cref="TablestoreConnectionProvider"/> instance using the provided configuration.
    /// It's suitable for scenarios where you don't need to share the connection provider with other components.
    /// </para>
    /// <para>
    /// The connection provider will be disposed when this outbox instance is disposed.
    /// </para>
    /// </remarks>
    public TablestoreOutbox(TablestoreConfiguration configuration)
        : this(new TablestoreConnectionProvider(configuration), configuration)
    {
        
    }
    
    /// <summary>
    /// Initializes a new instance of the <see cref="TablestoreOutbox"/> class with the specified connection provider and configuration.
    /// </summary>
    /// <param name="connectionProvider">The connection provider to use for Table Store operations.</param>
    /// <param name="configuration">The configuration settings for the outbox.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionProvider"/> or <paramref name="configuration"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when the outbox table configuration is missing or invalid.</exception>
    /// <remarks>
    /// <para>
    /// This constructor allows sharing the connection provider with other components (like an inbox implementation),
    /// which is recommended for better resource utilization and connection pooling.
    /// </para>
    /// <para>
    /// The caller is responsible for disposing the connection provider when it's no longer needed.
    /// </para>
    /// </remarks>
    public TablestoreOutbox(IAmATablestoreConnectionProvider connectionProvider, TablestoreConfiguration configuration)
    {
        _connectionProvider = connectionProvider;
        _configuration = configuration;

        if (configuration.Outbox == null || string.IsNullOrEmpty(configuration.Outbox.Name))
        {
            throw new ArgumentException("inbox collection can't be null or empty", nameof(configuration));
        }

        _table = configuration.Outbox;
    }

    /// <inheritdoc cref="IAmAnOutbox.Tracer" />
    public IAmABrighterTracer? Tracer { get; set; }

    
    /// <inheritdoc />
    public void Add(Message message, RequestContext? requestContext, int outBoxTimeout = -1,
        IAmABoxTransactionProvider<NoTransaction>? transactionProvider = null)
    {
        var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.message.id"] = message.Id
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName,
                BoxDbOperation.Add, 
                _table.Name,
                dbAttributes: dbAttributes),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var client = _connectionProvider.GetTablestoreClient();
            client.PutRow(new PutRowRequest(_table.Name, 
                new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                ToPrimaryKey(message),
                ToColumns(message)));
        }
        catch (OTSServerException ex) when (ex.ErrorCode == ErrorCodeConditionCheckFail)
        {
            // Ignore duplicate inserts
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }
    
    /// <inheritdoc />
    public void Add(IEnumerable<Message> messages, RequestContext? requestContext, int outBoxTimeout = -1,
        IAmABoxTransactionProvider<NoTransaction>? transactionProvider = null)
    {
        foreach (var message in messages)
        {
            Add(message, requestContext, outBoxTimeout, transactionProvider);
        }
    }

    /// <inheritdoc />
    public void Delete(Id[] messageIds, RequestContext? requestContext, Dictionary<string, object>? args = null)
    {
        var spans = messageIds.ToDictionary(
            id => id.Value,
            id =>
            {
                var dbAttributes = new Dictionary<string, string>
                {
                    ["db.operation.parameter.message.id"] = id.Value
                };

                return Tracer?.CreateDbSpan(
                    new BoxSpanInfo(DbSystem.Firestore, 
                        _configuration.DatabaseName, 
                        BoxDbOperation.Delete,
                        _table.Name,
                        dbAttributes: dbAttributes),
                    requestContext?.Span,
                    options: _configuration.Instrumentation);
            });

        try
        {
            var client = _connectionProvider.GetTablestoreClient();
            foreach (var messageId in messageIds)
            {
                client.DeleteRow(new DeleteRowRequest(_table.Name,
                    new Condition(RowExistenceExpectation.IGNORE),
                    ToPrimaryKey(messageId)));
            }
        }
        catch (OTSServerException ex) when (ex.ErrorCode == ErrorCodeConditionCheckFail)
        {
            // Ignore duplicate inserts
        }
        finally
        {
            Tracer?.EndSpans(new ConcurrentDictionary<string, Activity>(spans.Where(x => x.Value != null)!));
        }
    }

    /// <inheritdoc />
    public IEnumerable<Message> DispatchedMessages(TimeSpan dispatchedSince, RequestContext? requestContext, int pageSize = 100,
        int pageNumber = 1, int outBoxTimeout = -1, Dictionary<string, object>? args = null)
    {
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.DispatchedMessages, 
                _table.Name),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var dispatchedAt = _configuration.TimeProvider.GetUtcNow().Subtract(dispatchedSince).ToUnixTimeMilliseconds();
            var query = new SearchQuery
            {
                Query = new RangeQuery(DispatchedAt,
                    new ColumnValue(0),
                    new ColumnValue(dispatchedAt)),
                Limit = pageSize,
                Offset = (pageNumber - 1) * pageSize,
                Sort = new Sort([new FieldSort(nameof(MessageHeader.TimeStamp), SortOrder.DESC)])
            };

            var client = _connectionProvider.GetTablestoreClient();
            var response = client.Search(new SearchRequest(_table.Name, IndexName, query)
            {
                ColumnsToGet = new ColumnsToGet { ReturnAll = true },
            });

            return response.Rows
                .Select(ToMessage)
                .ToList();
        }
        finally
        {
           Tracer?.EndSpan(span); 
        }
    }

    /// <inheritdoc />
    public Message Get(Id messageId, RequestContext? requestContext, int outBoxTimeout = -1, Dictionary<string, object>? args = null)
    {
        var dbAttributes = new Dictionary<string, string>
        {
            {"db.operation.parameter.message.id", messageId.Value }
        };
        
       var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.Get, 
                _table.Name,
                dbAttributes: dbAttributes),
            requestContext?.Span,
            options: _configuration.Instrumentation);

       try
       {
           var client = _connectionProvider.GetTablestoreClient();
           var response = client.GetRow(new GetRowRequest(new SingleRowQueryCriteria(_table.Name)
           {
               RowPrimaryKey = ToPrimaryKey(messageId),
           }));

           if (response.PrimaryKey.Count == 0)
           {
               return new Message();
           }

           return ToMessage(response.Row);
       }
       finally
       {
           Tracer?.EndSpan(span); 
       }
    }
    
    public IEnumerable<Message> Get(RequestContext? requestContext = null,
        int pageSize = 100,
        int pageNumber = 1)
    {
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName,
                BoxDbOperation.Get,
                _table.Name),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var request = new SearchRequest(_table.Name, IndexName, new SearchQuery
            {
                Offset =  (pageNumber - 1) * pageSize,
                Limit = pageSize,
            })
            {
                ColumnsToGet =  new ColumnsToGet { ReturnAll = true },
            };
            
            var client = _connectionProvider.GetTablestoreClient();
            var response = client.Search(request);
            if (response.Rows.Count == 0)
            {
                return [];
            }
            
            return response.Rows
                .Select(item => ToMessage((Row)item))
                .ToList();
        }
        finally
        {
            Tracer?.EndSpan(span); 
        }   
    }

    /// <inheritdoc />
    public IEnumerable<Message> Get(IEnumerable<Id> messageIds, RequestContext? requestContext, int outBoxTimeout = -1, Dictionary<string, object>? args = null)
    {
        messageIds = messageIds.ToList();
        var ids = messageIds.Select(id => id.Value).ToArray();
        
        var dbAttributes = new Dictionary<string, string>
        {
            {"db.operation.parameter.message.ids", string.Join(",", ids)}
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName,
                BoxDbOperation.Get,
                _table.Name,
                dbAttributes: dbAttributes),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var request = new BatchGetRowRequest();
            request.Add(_table.Name, messageIds.Select(ToPrimaryKey).ToList());
            
            var client = _connectionProvider.GetTablestoreClient();
            var response = client.BatchGetRow(request);
            if (!response.RowDataGroupByTable.TryGetValue(_table.Name, out var value))
            {
                return [];
            }
            
            return value
                .Where(item => item.IsOK)
                .Select(item => ToMessage((Row)item.Row))
                .ToList();
        }
        finally
        {
           Tracer?.EndSpan(span); 
        }
    }

    /// <inheritdoc />
    public void MarkDispatched(Id id, RequestContext? requestContext, DateTimeOffset? dispatchedAt = null, Dictionary<string, object>? args = null)
    {
        var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.message.id"] = id.Value
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.MarkDispatched, 
                _table.Name,
                dbAttributes: dbAttributes),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var client = _connectionProvider.GetTablestoreClient();
            var val = dispatchedAt?.ToUnixTimeMilliseconds() ?? _configuration.TimeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            
            var columns = new UpdateOfAttribute();
            columns.AddAttributeColumnToPut(DispatchedAt, new ColumnValue(val));
            
            client.UpdateRow(new UpdateRowRequest(_table.Name,
                new Condition(RowExistenceExpectation.IGNORE),
                ToPrimaryKey(id),
                columns));
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <inheritdoc />
    public IEnumerable<Message> OutstandingMessages(TimeSpan dispatchedSince, RequestContext? requestContext, int pageSize = 100,
        int pageNumber = 1, IEnumerable<RoutingKey>? trippedTopics = null, Dictionary<string, object>? args = null)
    {
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.OutStandingMessages, 
                _table.Name),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var isNotDispatched = new TermQuery(DispatchedAt, new ColumnValue(-1));
            var timestampSince = new RangeQuery(nameof(MessageHeader.TimeStamp),
                new ColumnValue(0),
                new ColumnValue(_configuration.TimeProvider.GetUtcNow().Subtract(dispatchedSince).ToUnixTimeMilliseconds()));

            var andQuery = new BoolQuery
            {
                MustQueries = [
                    isNotDispatched,
                    timestampSince
                ]
            };

            var query = new SearchQuery
            {
                Query = andQuery,
                Sort = new Sort([new FieldSort(nameof(MessageHeader.TimeStamp), SortOrder.DESC)]),
                Limit = pageSize,
                Offset = (pageNumber - 1) * pageSize,
            };
            
            var client = _connectionProvider.GetTablestoreClient();
            var response = client.Search(new SearchRequest(_table.Name, IndexName, query)
            {
                ColumnsToGet = new ColumnsToGet
                {
                    ReturnAll = true
                }
            });
            
            return response.Rows
                .Select(ToMessage)
                .ToList();
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <inheritdoc />
    public int GetOutstandingMessageCount(TimeSpan dispatchedSince, RequestContext? requestContext, int maxCount = 100,
        Dictionary<string, object>? args = null)
    {
         var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.OutStandingMessageCount, 
                _table.Name),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var isNotDispatched = new TermQuery(DispatchedAt, new ColumnValue(-1));
            var timestampSince = new RangeQuery(nameof(MessageHeader.TimeStamp),
                new ColumnValue(0),
                new ColumnValue(_configuration.TimeProvider.GetUtcNow().Subtract(dispatchedSince).ToUnixTimeMilliseconds()));

            var andQuery = new BoolQuery
            {
                MustQueries = [isNotDispatched, timestampSince]
            };

            var query = new SearchQuery
            {
                Query = andQuery,
                Sort = new Sort([new FieldSort(nameof(MessageHeader.TimeStamp), SortOrder.DESC)]),
                GetTotalCount = true
            };
            
            var client = _connectionProvider.GetTablestoreClient();
            var response = client.Search(new SearchRequest(_table.Name, IndexName, query)
            {
                ColumnsToGet = new ColumnsToGet
                {
                    ReturnAll = false 
                }
            });
            
            return Convert.ToInt32(response.TotalCount);
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }


    private static PrimaryKey ToPrimaryKey(Message message)
    {
        return ToPrimaryKey(message.Id);
    }
    
    private static PrimaryKey ToPrimaryKey(Id id)
    {
        return new PrimaryKey
        {
            ["Id"] = new ColumnValue(id.Value)
        };
    }

    private static AttributeColumns ToColumns(Message message)
    {
        var attrs = new AttributeColumns();
        attrs[DispatchedAt] = new ColumnValue(-1);
        attrs[nameof(MessageHeader.Topic)] = new ColumnValue(message.Header.Topic.Value);
        attrs[nameof(MessageHeader.MessageType)] = new ColumnValue(message.Header.MessageType.ToString());
        attrs[nameof(MessageHeader.SpecVersion)] = new ColumnValue(message.Header.SpecVersion);
        attrs[nameof(MessageHeader.Source)] = new ColumnValue(message.Header.Source.ToString());
        attrs[nameof(MessageHeader.Topic)] = new ColumnValue(message.Header.Topic.ToString());
        attrs[nameof(MessageHeader.TimeStamp)] = new ColumnValue(message.Header.TimeStamp.ToUnixTimeMilliseconds());
        attrs[nameof(Message.Body)] = new ColumnValue(message.Body.Bytes);
        attrs[nameof(MessageHeader.ContentType)] = new ColumnValue(message.Header.ContentType.ToString());
        attrs[nameof(MessageHeader.Baggage)] = new ColumnValue(message.Header.Baggage.ToString());
        attrs[nameof(MessageHeader.Bag)] = new ColumnValue(JsonSerializer.Serialize(message.Header.Bag, JsonSerialisationOptions.Options));

        if (message.Header.Type != CloudEventsType.Empty)
        {
            attrs[nameof(MessageHeader.Type)] = new ColumnValue(message.Header.Type.ToString());
        }
        
        if (!Id.IsNullOrEmpty(message.Header.CorrelationId))
        {
            attrs[nameof(Message.Header.CorrelationId)] = new ColumnValue(message.Header.CorrelationId.Value);
        }
        
        if (!string.IsNullOrEmpty(message.Header.DataRef))
        {
            attrs[nameof(MessageHeader.Type)] = new ColumnValue(message.Header.DataRef);
        }
        
        if (message.Header.DataSchema != null)
        {
            attrs[nameof(MessageHeader.DataSchema)] = new ColumnValue(message.Header.DataSchema.ToString());
        }
        
        if (!PartitionKey.IsNullOrEmpty(message.Header.PartitionKey))
        {
            attrs[nameof(Message.Header.PartitionKey)] = new ColumnValue(message.Header.PartitionKey.Value);
        }
        
        if (!RoutingKey.IsNullOrEmpty(message.Header.ReplyTo))
        {
            attrs[nameof(Message.Header.ReplyTo)] = new ColumnValue(message.Header.ReplyTo.Value);
        }
        
        if (!string.IsNullOrEmpty(message.Header.Subject))
        {
            attrs[nameof(MessageHeader.Subject)] = new ColumnValue(message.Header.Subject);
        }
        
        if (!TraceState.IsNullOrEmpty(message.Header.TraceState))
        {
            attrs[nameof(MessageHeader.TraceState)] = new ColumnValue(message.Header.TraceState!.Value);
        }
        
        if (!TraceParent.IsNullOrEmpty(message.Header.TraceParent))
        {
            attrs[nameof(MessageHeader.TraceParent)] = new ColumnValue(message.Header.TraceParent!.Value);
        }
        
        if (!Id.IsNullOrEmpty(message.Header.WorkflowId))
        {
            attrs[nameof(MessageHeader.WorkflowId)] = new ColumnValue(message.Header.WorkflowId.Value);
        }
        
        if (!Id.IsNullOrEmpty(message.Header.JobId))
        {
            attrs[nameof(MessageHeader.JobId)] = new ColumnValue(message.Header.JobId.Value);
        }
        
        return attrs;
    }
    
    private Message ToMessage(Row row)
    {
        return new Message(
            new MessageHeader(
                messageId: GetId(row),
                topic: GetTopic(row),
                messageType: GetMessageType(row),
                source: GetSource(row),
                type: GetCloudEventsType(row),
                timeStamp: GetTimestamp(row, _configuration.TimeProvider),
                correlationId: GetCorrelationId(row),
                replyTo: GetReplyTo(row),
                contentType: GetContentType(row),
                partitionKey: GetPartitionKey(row),
                dataSchema: GetDataSchema(row),
                subject: GetSubject(row),
                workflowId: GetWorkflowId(row),
                jobId: GetJobId(row),
                delayed: GetDelay(row),
                traceParent: GetTraceParent(row),
                traceState: GetTraceState(row),
                baggage: GetBaggage(row))
            {
                SpecVersion = GetSpecVersion(row),
                Bag = GetHeaders(row)
            },
            new MessageBody(GetBody(row)));
        
        static Id GetId(Row row)
        {
            if (row.PrimaryKey.TryGetValue("Id", out var value))
            {
                return Id.Create(value.StringValue);
            }
            
            throw new ArgumentException($"Row {row} has no primary key");
        }
        
        static RoutingKey GetTopic(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.Topic), out var value))
            {
                return new RoutingKey(value.StringValue);
            }
            
            throw new ArgumentException($"Row {row} has no Topic");
        }
        
        static MessageType GetMessageType(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.MessageType), out var value)
                && Enum.TryParse<MessageType>(value.StringValue, true, out var messageType))
            {
                return messageType;
            }
            
            throw new ArgumentException($"Row {row} has no MessageTyp");
        }
        
        static Uri? GetSource(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.Source), out var value))
            {
                return new Uri(value.StringValue, UriKind.RelativeOrAbsolute);
            }

            return null;
        }
        
        static CloudEventsType? GetCloudEventsType(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.Type), out var value))
            {
                return new CloudEventsType(value.StringValue);
            }

            return null;
        }
        
        static DateTimeOffset GetTimestamp(Row row, TimeProvider timeProvider)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.TimeStamp), out var value))
            {
                return DateTimeOffset.FromUnixTimeMilliseconds(value.IntegerValue);
            }

            return timeProvider.GetUtcNow();
        }
        
        static Id? GetCorrelationId(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.CorrelationId), out var value))
            {
                return Id.Create(value.StringValue);
            }

            return null;
        }
        
        static RoutingKey? GetReplyTo(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.ReplyTo), out var value))
            {
                return new RoutingKey(value.StringValue);
            }

            return null;
        }
        
        static ContentType? GetContentType(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.ContentType), out var value))
            {
                return new ContentType(value.StringValue);
            }

            return null;
        }
        
        static PartitionKey? GetPartitionKey(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.PartitionKey), out var value))
            {
                return new PartitionKey(value.StringValue);
            }

            return null;
        }
        
        static Uri? GetDataSchema(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.DataSchema), out var value))
            {
                return new Uri(value.StringValue, UriKind.RelativeOrAbsolute);
            }

            return null;
        }
        
        static string? GetSubject(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.Subject), out var value))
            {
                return value.StringValue;
            }

            return null;
        }
        
        static string GetSpecVersion(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.SpecVersion), out var value))
            {
                return value.StringValue;
            }

            return MessageHeader.DefaultSpecVersion;
        }
        
        static Id? GetWorkflowId(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.WorkflowId), out var value))
            {
                return Id.Create(value.StringValue);
            }

            return null;
        }
        
        static Id? GetJobId(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.JobId), out var value))
            {
                return Id.Create(value.StringValue);
            }

            return null;
        }
        
        static TraceParent? GetTraceParent(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.TraceParent), out var value))
            {
                return new TraceParent(value.StringValue);
            }

            return null;
        }
        
        static TraceState? GetTraceState(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.TraceState), out var value))
            {
                return new TraceState(value.StringValue);
            }

            return null;
        }
        
        static Baggage GetBaggage(Row row)
        {
            var baggage = new Baggage();
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.Baggage), out var value))
            {
                baggage.LoadBaggage(value.StringValue);
            }

            return baggage;
        }
        
        static Dictionary<string, object> GetHeaders(Row row)
        {
            if (row.AttributeColumns.TryGetValue(nameof(MessageHeader.Bag), out var value))
            {
                return new Dictionary<string, object>(JsonSerializer.Deserialize<Dictionary<string, object>>(value.StringValue, JsonSerialisationOptions.Options)!);
            }

            return new Dictionary<string, object>();
        }
        
        static TimeSpan? GetDelay(Row row)
        {
            return null;
        }

        
        static byte[] GetBody(Row row)
        {
            if (row.AttributeColumns.TryGetValue("Body", out var value))
            {
                return value.BinaryValue;
            }

            return [];
        }
    }

    private string IndexName
    {
        get
        {
            if (string.IsNullOrEmpty(_table.IndexName))
            {
                return $"IX_{_table.Name}_Outstanding";
            }

            return _table.IndexName!;
        }
    }

    /// <inheritdoc />
    public async Task AddAsync(Message message, RequestContext? requestContext, int outBoxTimeout = -1,
        IAmABoxTransactionProvider<NoTransaction>? transactionProvider = null,
        CancellationToken cancellationToken = new())
    {
        var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.message.id"] = message.Id
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName,
                BoxDbOperation.Add, 
                _table.Name,
                dbAttributes: dbAttributes),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var client = _connectionProvider.GetTablestoreClient();
            await client.PutRowAsync(new PutRowRequest(_table.Name, 
                new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                ToPrimaryKey(message),
                ToColumns(message)))
                .ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (OTSServerException ex) when (ex.ErrorCode == ErrorCodeConditionCheckFail)
        {
            // Ignore duplicate inserts
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <inheritdoc />
    public async Task AddAsync(IEnumerable<Message> messages, RequestContext? requestContext, int outBoxTimeout = -1,
        IAmABoxTransactionProvider<NoTransaction>? transactionProvider = null,
        CancellationToken cancellationToken = new())
    {
        foreach (var message in messages)
        {
            await AddAsync(message, requestContext, outBoxTimeout, transactionProvider, cancellationToken)
                .ConfigureAwait(ContinueOnCapturedContext);
        }
    }

    /// <inheritdoc />
    public async Task DeleteAsync(Id[] messageIds, RequestContext requestContext, Dictionary<string, object>? args = null,
        CancellationToken cancellationToken = new())
    {
        var spans = messageIds.ToDictionary(
            id => id.Value,
            id =>
            {
                var dbAttributes = new Dictionary<string, string>
                {
                    ["db.operation.parameter.message.id"] = id.Value
                };

                return Tracer?.CreateDbSpan(
                    new BoxSpanInfo(DbSystem.Firestore, 
                        _configuration.DatabaseName, 
                        BoxDbOperation.Delete,
                        _table.Name,
                        dbAttributes: dbAttributes),
                    requestContext?.Span,
                    options: _configuration.Instrumentation);
            });

        try
        {
            var client = _connectionProvider.GetTablestoreClient();
            foreach (var messageId in messageIds)
            {
                await client.DeleteRowAsync(new DeleteRowRequest(_table.Name,
                    new Condition(RowExistenceExpectation.IGNORE),
                    ToPrimaryKey(messageId)))
                    .ConfigureAwait(ContinueOnCapturedContext);
            }
        }
        catch (OTSServerException ex) when (ex.ErrorCode == ErrorCodeConditionCheckFail)
        {
            // Ignore duplicate inserts
        }
        finally
        {
            Tracer?.EndSpans(new ConcurrentDictionary<string, Activity>(spans.Where(x => x.Value != null)!));
        }
    }

    /// <inheritdoc />
    public async Task<IEnumerable<Message>> DispatchedMessagesAsync(TimeSpan dispatchedSince, RequestContext requestContext, int pageSize = 100,
        int pageNumber = 1, int outboxTimeout = -1, Dictionary<string, object>? args = null,
        CancellationToken cancellationToken = new())
    {
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.DispatchedMessages, 
                _table.Name),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var dispatchedAt = _configuration.TimeProvider.GetUtcNow().Subtract(dispatchedSince).ToUnixTimeMilliseconds();
            var query = new SearchQuery
            {
                Query = new RangeQuery(DispatchedAt,
                    new ColumnValue(0),
                    new ColumnValue(dispatchedAt)),
                Limit = pageSize,
                Offset = (pageNumber - 1) * pageSize,
                Sort = new Sort([new FieldSort(nameof(MessageHeader.TimeStamp), SortOrder.DESC)])
            };

            var client = _connectionProvider.GetTablestoreClient();
            var response = await client.SearchAsync(new SearchRequest(_table.Name, IndexName, query)
            {
                ColumnsToGet = new ColumnsToGet { ReturnAll = true },
            }).ConfigureAwait(ContinueOnCapturedContext);

            return response.Rows
                .Select(ToMessage)
                .ToList();
        }
        finally
        {
            Tracer?.EndSpan(span); 
        }
    }

    /// <inheritdoc />
    public async Task<Message> GetAsync(Id messageId, RequestContext requestContext, int outBoxTimeout = -1, Dictionary<string, object>? args = null,
        CancellationToken cancellationToken = new())
    {
        var dbAttributes = new Dictionary<string, string>
        {
            {"db.operation.parameter.message.id", messageId.Value }
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.Get, 
                _table.Name,
                dbAttributes: dbAttributes),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var client = _connectionProvider.GetTablestoreClient();
            var response = await client.GetRowAsync(new GetRowRequest(new SingleRowQueryCriteria(_table.Name)
            {
                RowPrimaryKey = ToPrimaryKey(messageId),
            }));

            if (response.PrimaryKey.Count == 0)
            {
                return new Message();
            }

            return ToMessage(response.Row);
        }
        finally
        {
            Tracer?.EndSpan(span); 
        }
    }
    
    public async Task<IEnumerable<Message>> GetAsync(RequestContext? requestContext = null,
        int pageSize = 100,
        int pageNumber = 1,
        CancellationToken cancellationToken = new())
    {
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName,
                BoxDbOperation.Get,
                _table.Name),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var request = new SearchRequest(_table.Name, IndexName, new SearchQuery
            {
                Offset =  (pageNumber - 1) * pageSize,
                Limit = pageSize,
            })
            {
                ColumnsToGet =  new ColumnsToGet { ReturnAll = true },
            };
            
            var client = _connectionProvider.GetTablestoreClient();
            var response = await client.SearchAsync(request);
            if (response.Rows.Count == 0)
            {
                return [];
            }
            
            return response.Rows
                .Select(item => ToMessage((Row)item))
                .ToList();
        }
        finally
        {
            Tracer?.EndSpan(span); 
        }   
    }

    /// <inheritdoc />
    public async Task<IEnumerable<Message>> GetAsync(IEnumerable<Id> messageId, RequestContext requestContext, int outBoxTimeout = -1, Dictionary<string, object>? args = null,
        CancellationToken cancellationToken = new())
    {
        var messageIds = messageId.ToList();
        var ids = messageIds.Select(id => id.Value).ToArray();
        
        var dbAttributes = new Dictionary<string, string>
        {
            {"db.operation.parameter.message.ids", string.Join(",", ids)}
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName,
                BoxDbOperation.Get,
                _table.Name,
                dbAttributes: dbAttributes),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var request = new BatchGetRowRequest();
            request.Add(_table.Name, messageIds.Select(ToPrimaryKey).ToList());
            
            var client = _connectionProvider.GetTablestoreClient();
            var response = await client.BatchGetRowAsync(request);
            if (!response.RowDataGroupByTable.TryGetValue(_table.Name, out var value))
            {
                return [];
            }
            
            return value
                .Where(item => item.IsOK)
                .Select(item => ToMessage((Row)item.Row))
                .ToList();
        }
        finally
        {
            Tracer?.EndSpan(span); 
        }
    }

    /// <inheritdoc />
    public async Task MarkDispatchedAsync(Id id, RequestContext requestContext, DateTimeOffset? dispatchedAt = null,
        Dictionary<string, object>? args = null, CancellationToken cancellationToken = new())
    {
        var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.message.id"] = id.Value
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.MarkDispatched, 
                _table.Name,
                dbAttributes: dbAttributes),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var client = _connectionProvider.GetTablestoreClient();
            var val = dispatchedAt?.ToUnixTimeMilliseconds() ?? _configuration.TimeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            
            var columns = new UpdateOfAttribute();
            columns.AddAttributeColumnToPut(DispatchedAt, new ColumnValue(val));
            
            await client.UpdateRowAsync(new UpdateRowRequest(_table.Name,
                new Condition(RowExistenceExpectation.IGNORE),
                ToPrimaryKey(id),
                columns));
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <inheritdoc />
    public async Task MarkDispatchedAsync(IEnumerable<Id> ids, RequestContext requestContext, DateTimeOffset? dispatchedAt = null,
        Dictionary<string, object>? args = null, CancellationToken cancellationToken = new())
    {
        foreach (var id in ids)
        {
            await MarkDispatchedAsync(id, requestContext, dispatchedAt, args, cancellationToken)
                .ConfigureAwait(ContinueOnCapturedContext);
        }
    }

    /// <inheritdoc />
    public async Task<IEnumerable<Message>> OutstandingMessagesAsync(TimeSpan dispatchedSince, RequestContext requestContext, int pageSize = 100,
        int pageNumber = 1, IEnumerable<RoutingKey>? trippedTopics = null, Dictionary<string, object>? args = null,
        CancellationToken cancellationToken = new())
    {
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.OutStandingMessages, 
                _table.Name),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var isNotDispatched = new TermQuery(DispatchedAt, new ColumnValue(-1));
            var timestampSince = new RangeQuery(nameof(MessageHeader.TimeStamp),
                new ColumnValue(0),
                new ColumnValue(_configuration.TimeProvider.GetUtcNow().Subtract(dispatchedSince).ToUnixTimeMilliseconds()));

            var andQuery = new BoolQuery
            {
                MustQueries = [
                    isNotDispatched,
                    timestampSince
                ]
            };

            var query = new SearchQuery
            {
                Query = andQuery,
                Sort = new Sort([new FieldSort(nameof(MessageHeader.TimeStamp), SortOrder.DESC)]),
                Limit = pageSize,
                Offset = (pageNumber - 1) * pageSize,
            };
            
            var client = _connectionProvider.GetTablestoreClient();
            var response = await client.SearchAsync(new SearchRequest(_table.Name, IndexName, query)
            {
                ColumnsToGet = new ColumnsToGet
                {
                    ReturnAll = true
                }
            }).ConfigureAwait(ContinueOnCapturedContext);
            
            return response.Rows
                .Select(ToMessage)
                .ToList();
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }
    
    /// <inheritdoc />
    public async Task<int> GetOutstandingMessageCountAsync(TimeSpan dispatchedSince, RequestContext? requestContext, int maxCount = 100,
        Dictionary<string, object>? args = null, CancellationToken cancellationToken = new())
    {
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore, 
                _configuration.DatabaseName, 
                BoxDbOperation.OutStandingMessageCount, 
                _table.Name),
            requestContext?.Span,
            options: _configuration.Instrumentation);

        try
        {
            var isNotDispatched = new TermQuery(DispatchedAt, new ColumnValue(-1));
            var timestampSince = new RangeQuery(nameof(MessageHeader.TimeStamp),
                new ColumnValue(0),
                new ColumnValue(_configuration.TimeProvider.GetUtcNow().Subtract(dispatchedSince).ToUnixTimeMilliseconds()));

            var andQuery = new BoolQuery
            {
                MustQueries = [isNotDispatched, timestampSince]
            };

            var query = new SearchQuery
            {
                Query = andQuery,
                Sort = new Sort([new FieldSort(nameof(MessageHeader.TimeStamp), SortOrder.DESC)]),
                GetTotalCount = true
            };
            
            var client = _connectionProvider.GetTablestoreClient();
            var response = await client.SearchAsync(new SearchRequest(_table.Name, IndexName, query)
            {
                ColumnsToGet = new ColumnsToGet
                {
                    ReturnAll = false 
                }
            });
            
            return Convert.ToInt32(response.TotalCount);
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <inheritdoc />
    public bool ContinueOnCapturedContext { get; set; }
}