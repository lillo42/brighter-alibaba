using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Filter;
using Aliyun.OTS.Request;
using Brighter.Tablestore;
using Paramore.Brighter;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.Inbox.Exceptions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.Observability;

namespace Brighter.Inbox.Tablestore;

public class TablestoreInbox : IAmAnInboxSync, IAmAnInboxAsync
{
    private readonly IAmATablestoreConnectionProvider _connectionProvider;
    private readonly TablestoreConfiguration _configuration;
    private readonly TablestoreTable _table;
    
    public IAmABrighterTracer? Tracer { get; set; }

    public TablestoreInbox(TablestoreConfiguration configuration)
        : this(new TablestoreConnectionProvider(configuration), configuration)
    {
        
    }
    
    public TablestoreInbox(IAmATablestoreConnectionProvider connectionProvider, TablestoreConfiguration configuration)
    {
        _connectionProvider = connectionProvider;
        _configuration = configuration;

        if (configuration.Inbox == null || string.IsNullOrEmpty(configuration.Inbox.Name))
        {
            throw new ArgumentException("inbox collection can't be null or empty", nameof(configuration));
        }

        _table = configuration.Inbox;
    }
    
    public void Add<T>(T command, string contextKey, RequestContext? requestContext, int timeoutInMilliseconds = -1) 
        where T : class, IRequest
    {
        var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.command.id"] = command.Id,
            ["db.operation.parameter.command.context_key"] = contextKey,
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore,
                _configuration.Configuration.InstanceName,
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
                ToPrimaryKey(command.Id), ToColumns(contextKey, command)));
        }
        catch (Exception e)
        {
            // Ignrore duplicate inserts
            Console.WriteLine(e);
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    public T Get<T>(string id, string contextKey, RequestContext? requestContext, int timeoutInMilliseconds = -1) where T : class, IRequest
    {
        var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.command.id"] = id,
            ["db.operation.parameter.command.context_key"] = contextKey,
        };

        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore,
                _configuration.Configuration.InstanceName,
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
                RowPrimaryKey = ToPrimaryKey(id),
                Filter = new SingleColumnValueFilter("ContextKey", CompareOperator.EQUAL, new ColumnValue(contextKey))
            }));

            return JsonSerializer.Deserialize<T>(response.Row.AttributeColumns["Body"].BinaryValue,
                JsonSerialisationOptions.Options)!;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw new RequestNotFoundException<T>(id);
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    public bool Exists<T>(string id, string contextKey, RequestContext? requestContext, int timeoutInMilliseconds = -1) where T : class, IRequest
    {
         var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.command.id"] = id,
            ["db.operation.parameter.command.context_key"] = contextKey,
        };

        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore,
                _configuration.Configuration.InstanceName,
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
                RowPrimaryKey = ToPrimaryKey(id),
                Filter = new SingleColumnValueFilter("ContextKey", CompareOperator.EQUAL, new ColumnValue(contextKey))
            }));

            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return false;
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    private static PrimaryKey ToPrimaryKey(Id value) => ToPrimaryKey(value.Value);
    private static PrimaryKey ToPrimaryKey(string value)
    {
        return new PrimaryKey
        {
            { "Id", new ColumnValue(value) }
        };
    }


    private AttributeColumns ToColumns<T>(string contextKey, T request)
    {
        var columns = new AttributeColumns();
        columns.Add("Body", new ColumnValue(JsonSerializer.SerializeToUtf8Bytes(request, JsonSerialisationOptions.Options)));
        columns.Add("ContextKey", new ColumnValue(contextKey));
        columns.Add("Timestamp", new ColumnValue(_configuration.TimeProvider.GetUtcNow().ToRfc3339()));
        columns.Add("Type", new ColumnValue(typeof(T).FullName));
        long ttl = -1;
        if (_table.TimeToLive.HasValue)
        {
            ttl = Convert.ToInt64(_table.TimeToLive.Value.TotalSeconds);
        }
        
        columns.Add("Ttl", new ColumnValue(ttl));
        return columns;
    }

    public async Task AddAsync<T>(T command, string contextKey, RequestContext? requestContext, int timeoutInMilliseconds = -1,
        CancellationToken cancellationToken = new CancellationToken()) where T : class, IRequest
    {
        var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.command.id"] = command.Id,
            ["db.operation.parameter.command.context_key"] = contextKey,
        };
        
        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore,
                _configuration.Configuration.InstanceName,
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
                ToPrimaryKey(command.Id), ToColumns(contextKey, command)))
                .ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (Exception e)
        {
            // Ignrore duplicate inserts
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    public async Task<T> GetAsync<T>(string id, string contextKey, RequestContext? requestContext, int timeoutInMilliseconds = -1,
        CancellationToken cancellationToken = new CancellationToken()) where T : class, IRequest
    {
       var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.command.id"] = id,
            ["db.operation.parameter.command.context_key"] = contextKey,
        };

        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore,
                _configuration.Configuration.InstanceName,
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
                RowPrimaryKey = ToPrimaryKey(id),
                Filter = new SingleColumnValueFilter("ContextKey", CompareOperator.EQUAL, new ColumnValue(contextKey))
            }))
                .ConfigureAwait(ContinueOnCapturedContext);

            return JsonSerializer.Deserialize<T>(response.Row.AttributeColumns["Body"].BinaryValue,
                JsonSerialisationOptions.Options)!;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw new RequestNotFoundException<T>(id);
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    public async Task<bool> ExistsAsync<T>(string id, string contextKey, RequestContext? requestContext, int timeoutInMilliseconds = -1,
        CancellationToken cancellationToken = new CancellationToken()) where T : class, IRequest
    {
        var dbAttributes = new Dictionary<string, string>
        {
            ["db.operation.parameter.command.id"] = id,
            ["db.operation.parameter.command.context_key"] = contextKey,
        };

        var span = Tracer?.CreateDbSpan(
            new BoxSpanInfo(DbSystem.Firestore,
                _configuration.Configuration.InstanceName,
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
                RowPrimaryKey = ToPrimaryKey(id),
                Filter = new SingleColumnValueFilter("ContextKey", CompareOperator.EQUAL, new ColumnValue(contextKey))
            })).ConfigureAwait(ContinueOnCapturedContext);

            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return false;
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    public bool ContinueOnCapturedContext { get; set; }
}