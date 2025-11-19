using System.Net.Mime;
using Brighter.Transformers.Alibaba.TestDoubles;
using Paramore.Brighter;
using Paramore.Brighter.Observability;

namespace Brighter.Transformers.Alibaba.Outbox;

public abstract class OutboxTest<TTransaction>
{
    protected abstract IAmAnOutboxSync<Message, TTransaction> Outbox { get; }

    protected List<Message> CreatedMessages { get; } = [];
    
    
    [Before(Test)]
    public virtual void CreateStore()
    {
    }
    
    
    [After(Test)]
    public virtual void DeleteStore()
    {
    }

    protected abstract IEnumerable<Message> GetAllMessages();
    
    protected abstract IAmABoxTransactionProvider<TTransaction> CreateTransactionProvider();

    protected virtual Message CreateRandomMessage(DateTimeOffset? timestamp = null)
    {
        var random = new Random();
        var messageHeader = new MessageHeader(
            messageId:    Id.Random(),
            topic:        new RoutingKey(Uuid.NewAsString()),
            messageType:  MessageType.MT_DOCUMENT,
            source:       new Uri(Uuid.NewAsString(), UriKind.Relative),
            type:         new CloudEventsType(Uuid.NewAsString()),
            timeStamp:    timestamp ?? DateTimeOffset.UtcNow,
            correlationId:Id.Random(),
            replyTo:      new RoutingKey(Uuid.NewAsString()),
            contentType:  new ContentType(MediaTypeNames.Text.Plain),
            partitionKey: Uuid.NewAsString(),
            dataSchema:   new Uri("https://schema.test"),
            subject:      Uuid.NewAsString(),
            handledCount: random.Next(),
            delayed:      TimeSpan.FromMilliseconds(5),
            traceParent:  "00-abcdef0123456789-abcdef0123456789-01",
            traceState:   "state123",
            baggage:      new Baggage(),
            workflowId: Id.Random(),
            jobId: Id.Random());

        messageHeader.Bag.Add("header1", Uuid.NewAsString());
        messageHeader.Bag.Add("header2", Uuid.NewAsString());
        messageHeader.Bag.Add("header3", Uuid.NewAsString());
        messageHeader.Bag.Add("header4", Uuid.NewAsString());
        messageHeader.Bag.Add("header5", Uuid.NewAsString());
        var message = new Message(messageHeader, new MessageBody(Uuid.NewAsString()));

        CreatedMessages.Add(message);
        return message;
    }

    [Test]
    public async Task When_Deleting_One_Message_It_Should_Be_Removed_From_Outbox()
    {
        // Arrange
        var context = new RequestContext();
        var firstMessage = CreateRandomMessage();
        var secondMessage = CreateRandomMessage();
        var thirdMessage = CreateRandomMessage();
        
        // Act
        Outbox.Add(firstMessage, context);
        Outbox.Add(secondMessage, context);
        Outbox.Add(thirdMessage, context);
        
        Outbox.Delete([firstMessage.Id], context);

        await Task.Delay(TimeSpan.FromSeconds(10));
        
        // Assert
        var messages = Outbox
            .OutstandingMessages(TimeSpan.Zero, context)
            .ToArray();

        await Assert.That(messages)
            .DoesNotContain(x => x.Id == firstMessage.Id)
            .And.Contains(x => x.Id == secondMessage.Id)
            .And.Contains(x => x.Id == thirdMessage.Id);
    }
    
    [Test]
    public async Task When_Deleting_Multiple_Messages_They_Should_Be_Removed_From_Outbox()
    {
        // Arrange
        var context = new RequestContext();
        var firstMessage = CreateRandomMessage();
        var secondMessage = CreateRandomMessage();
        var thirdMessage = CreateRandomMessage();
        
        // Act
        Outbox.Add(firstMessage, context);
        Outbox.Add(secondMessage, context);
        Outbox.Add(thirdMessage, context);
        
        Outbox.Delete([firstMessage.Id, secondMessage.Id, thirdMessage.Id], context);
        
        // Assert
        var messages = Outbox
            .OutstandingMessages(TimeSpan.Zero, context)
            .ToArray();
        
        
        await Assert.That(messages)
            .DoesNotContain(x => x.Id == firstMessage.Id)
            .And.DoesNotContain(x => x.Id == secondMessage.Id)
            .And.DoesNotContain(x => x.Id == thirdMessage.Id);
    }

    [Test]
    public async Task When_Retrieving_All_Messages_They_Should_Include_Dispatched_And_Undispatched()
    {
        // Arrange
        var context = new RequestContext();
        var earliest = CreateRandomMessage();
        var dispatched = CreateRandomMessage();
        var undispatched = CreateRandomMessage();
        
        Outbox.Add([earliest, dispatched, undispatched], context);
        Outbox.MarkDispatched(earliest.Id, context, DateTime.UtcNow.AddHours(-3));
        Outbox.MarkDispatched(dispatched.Id, context);
        
        await Task.Delay(TimeSpan.FromSeconds(10));
        
        // Act
        var messages = GetAllMessages().ToArray();

        // Assert
        await Assert.That(messages)
            .Contains(x => x.Id == earliest.Id)
            .And.Contains(x => x.Id == dispatched.Id)
            .And.Contains(x => x.Id == undispatched.Id);
    }
    
    [Test]
    public async Task When_Retrieving_Messages_By_Ids_It_Should_Return_Only_Requested_Messages()
    {
        // Arrange
        var context = new RequestContext();
        var earliest = CreateRandomMessage();
        var dispatched = CreateRandomMessage();
        var undispatched = CreateRandomMessage();
        
        Outbox.Add([earliest, dispatched, undispatched], context);
        Outbox.MarkDispatched(earliest.Id, context, DateTime.UtcNow.AddHours(-3));
        Outbox.MarkDispatched(dispatched.Id, context);

        await Task.Delay(TimeSpan.FromSeconds(10));
        
        // Act
        var messages =  Outbox
            .Get([earliest.Id, undispatched.Id], context)
            .ToArray();

        // Assert
        await Assert.That(messages)
            .Contains(x => x.Id == earliest.Id)
            .And.DoesNotContain(x => x.Id == dispatched.Id)
            .And.Contains(x => x.Id == undispatched.Id);
    }
    
    [Test]
    public async Task When_Retrieving_A_Message_By_Id_It_Should_Return_The_Correct_Message()
    {
        // Arrange
        var context = new RequestContext();
        var earliest = CreateRandomMessage();
        var dispatched = CreateRandomMessage();
        var undispatched = CreateRandomMessage();
        
        Outbox.Add([earliest, dispatched, undispatched], context);
        Outbox.MarkDispatched(earliest.Id, context, DateTime.UtcNow.AddHours(-3));
        Outbox.MarkDispatched(dispatched.Id, context);
        
        // Act
        var message = Outbox.Get(dispatched.Id, context);

        // Assert
        await Assert.That(message.Header.MessageId.Value)
            .IsEquivalentTo(dispatched.Header.MessageId.Value);
    }

    [Test]
    public async Task When_Retrieving_Dispatched_Messages_It_Should_Filter_By_Age()
    {
        // Arrange
        var context = new RequestContext();
        var earliest = CreateRandomMessage();
        var dispatched = CreateRandomMessage();
        var undispatched = CreateRandomMessage();
        
        Outbox.Add([earliest, dispatched, undispatched], context);
        Outbox.MarkDispatched(earliest.Id, context, DateTime.UtcNow.AddHours(-3));
        Outbox.MarkDispatched(dispatched.Id, context);
        
        await Task.Delay(TimeSpan.FromSeconds(10));
        
        // Act
        var allDispatched = Outbox.DispatchedMessages(TimeSpan.Zero, context).ToArray();
        var messagesOverAnHour  = Outbox.DispatchedMessages(TimeSpan.FromHours(1), context).ToArray();
        var messagesOver4Hours   = Outbox.DispatchedMessages(TimeSpan.FromHours(4), context).ToArray();
        
        // Assert
        await Assert.That(allDispatched)
            .Contains(x => x.Id == earliest.Id)
            .And.Contains(x => x.Id == dispatched.Id)
            .And.DoesNotContain(x => x.Id == undispatched.Id);
        
        await Assert.That(messagesOverAnHour)
            .Contains(x => x.Id == earliest.Id)
            .And.DoesNotContain(x => x.Id == dispatched.Id)
            .And.DoesNotContain(x => x.Id == undispatched.Id);
        
        await Assert.That(messagesOver4Hours)
            .DoesNotContain(x => x.Id == earliest.Id)
            .And.DoesNotContain(x => x.Id == dispatched.Id)
            .And.DoesNotContain(x => x.Id == undispatched.Id);
    }

    [Test]
    public async Task When_Retrieving_Outstanding_Messages_It_Should_Filter_By_Age()
    {
        // Arrange
        var context = new RequestContext();
        var earliest = CreateRandomMessage(DateTimeOffset.UtcNow.AddHours(-3));
        var dispatched = CreateRandomMessage();
        var undispatched = CreateRandomMessage();
        
        Outbox.Add([earliest, dispatched, undispatched], context);
        Outbox.MarkDispatched(dispatched.Id, context);
        
        await Task.Delay(TimeSpan.FromSeconds(10));
        
        // Act
        var allUndispatched = Outbox.OutstandingMessages(TimeSpan.Zero, context).ToArray();
        var messagesOverAnHour = Outbox.OutstandingMessages(TimeSpan.FromHours(1), context).ToArray();
        var messagesOver4Hours = Outbox.OutstandingMessages(TimeSpan.FromHours(4), context).ToArray();
        
        // Assert
        await Assert.That(allUndispatched)
            .Contains(x => x.Id == earliest.Id)
            .And.DoesNotContain(x => x.Id == dispatched.Id)
            .And.Contains(x => x.Id == undispatched.Id);
        
        await Assert.That(messagesOverAnHour)
            .Contains(x => x.Id == earliest.Id)
            .And.DoesNotContain(x => x.Id == dispatched.Id)
            .And.DoesNotContain(x => x.Id == undispatched.Id);
        
        await Assert.That(messagesOver4Hours)
            .DoesNotContain(x => x.Id == earliest.Id)
            .And.DoesNotContain(x => x.Id == dispatched.Id)
            .And.DoesNotContain(x => x.Id == undispatched.Id);
    }

    [Test]
    public async Task When_Retrieving_A_Non_Existent_Message_It_Should_Return_Empty_Message()
    {
        // Arrange
        var context = new RequestContext();
        
        // Act
        var message = Outbox.Get(Id.Random(), context);
        
        // Assert
        await Assert.That(message.Header.MessageType)
            .IsEqualTo(MessageType.MT_NONE);
    }

    [Test]
    public async Task When_Adding_A_Duplicate_Message_It_Should_Not_Throw()
    {
        // Arrange
        var context = new RequestContext();
        var message = CreateRandomMessage();
        Outbox.Add(message, context);
        
        // Act
        await Assert.That(() => Outbox.Add(message, context))
            .ThrowsNothing();
        
        // Assert
    }

    [Test]
    public async Task When_Adding_A_Message_It_Should_Be_Stored_With_All_Properties()
    {
        // Arrange
        var context = new RequestContext();
        var message = CreateRandomMessage();
        
        // Act
        Outbox.Add(message, context);
        var storedMessage = Outbox.Get(message.Id, context);
        
        // Assert
        await Assert.That(storedMessage.Body.Value).IsEquivalentTo(message.Body.Value);
        
        //should read the header from the sql outbox
        await Assert.That(storedMessage.Header.Topic).IsEqualTo(message.Header.Topic);
        await Assert.That(storedMessage.Header.MessageType).IsEqualTo(message.Header.MessageType);
        await Assert.That(storedMessage.Header.TimeStamp.ToString("yyyy-MM-ddTHH:mm:ss")).IsEqualTo(message.Header.TimeStamp.ToString("yyyy-MM-ddTHH:mm:ss"));
        await Assert.That(storedMessage.Header.HandledCount).IsEqualTo(0);
        await Assert.That(storedMessage.Header.Delayed).IsEqualTo(TimeSpan.Zero);
        await Assert.That(storedMessage.Header.CorrelationId.Value).IsEquivalentTo(message.Header.CorrelationId.Value);
        await Assert.That(storedMessage.Header.ReplyTo).IsEquivalentTo(message.Header.ReplyTo?.Value);
        await Assert.That(storedMessage.Header.ContentType.ToString()).StartsWith(message.Header.ContentType.ToString());
        await Assert.That(storedMessage.Header.PartitionKey).StartsWith(message.Header.PartitionKey.ToString());
        
        //Bag serialization
        await Assert.That(storedMessage.Header.Bag).HasCount(message.Header.Bag.Count);
        foreach (var (key, val) in message.Header.Bag)
        {
            await Assert.That(storedMessage.Header.Bag).ContainsKey(key);
            await Assert.That(storedMessage.Header.Bag[key].ToString()).IsEqualTo(val.ToString());
        }
            
        //Asserts for workflow properties
        await Assert.That(storedMessage.Header.WorkflowId!.Value).IsEquivalentTo(message.Header.WorkflowId!.Value);
        await Assert.That(storedMessage.Header.JobId!.Value).IsEquivalentTo(message.Header.JobId!.Value);

        // new fields assertions
        await Assert.That(storedMessage.Header.Source.ToString()).IsEquivalentTo(message.Header.Source.ToString());
        await Assert.That(storedMessage.Header.Type.Value).IsEquivalentTo(message.Header.Type.Value);
        await Assert.That(storedMessage.Header.DataSchema!.ToString()).IsEquivalentTo(message.Header.DataSchema?.ToString());
        await Assert.That(storedMessage.Header.Subject).IsEquivalentTo(message.Header.Subject!);
        await Assert.That(storedMessage.Header.TraceParent!.Value).IsEquivalentTo(message.Header.TraceParent?.Value);
        await Assert.That(storedMessage.Header.TraceState!.Value).IsEquivalentTo(message.Header.TraceState?.Value);
    }
    
    [Test]
    public virtual async Task When_Adding_A_Message_Within_Transaction_It_Should_Be_Stored_After_Commit()
    {
        // Arrange
        var transaction = CreateTransactionProvider();
        _ = transaction.GetTransaction();
        
        var message = CreateRandomMessage();
        var context = new RequestContext();
        
        
        // Act
        Outbox.Add(message, context, transactionProvider: transaction);
        transaction.Commit();
        
        var storedMessage = Outbox.Get(message.Id, context);
        
        // Assert
       await Assert.That(storedMessage.Body.Value).IsEquivalentTo(message.Body.Value);
        
        //should read the header from the sql outbox
        await Assert.That(storedMessage.Header.Topic).IsEqualTo(message.Header.Topic);
        await Assert.That(storedMessage.Header.MessageType).IsEqualTo(message.Header.MessageType);
        await Assert.That(storedMessage.Header.TimeStamp.ToString("yyyy-MM-ddTHH:mm:ss")).IsEqualTo(message.Header.TimeStamp.ToString("yyyy-MM-ddTHH:mm:ss"));
        await Assert.That(storedMessage.Header.HandledCount).IsEqualTo(0);
        await Assert.That(storedMessage.Header.Delayed).IsEqualTo(TimeSpan.Zero);
        await Assert.That(storedMessage.Header.CorrelationId.Value).IsEquivalentTo(message.Header.CorrelationId.Value);
        await Assert.That(storedMessage.Header.ReplyTo).IsEquivalentTo(message.Header.ReplyTo?.Value);
        await Assert.That(storedMessage.Header.ContentType.ToString()).StartsWith(message.Header.ContentType.ToString());
        await Assert.That(storedMessage.Header.PartitionKey).StartsWith(message.Header.PartitionKey.ToString());
        
        //Bag serialization
        await Assert.That(storedMessage.Header.Bag).HasCount(message.Header.Bag.Count);
        foreach (var (key, val) in message.Header.Bag)
        {
            await Assert.That(storedMessage.Header.Bag).ContainsKey(key);
            await Assert.That(storedMessage.Header.Bag[key].ToString()).IsEqualTo(val.ToString());
        }
            
        //Asserts for workflow properties
        await Assert.That(storedMessage.Header.WorkflowId!.Value).IsEquivalentTo(message.Header.WorkflowId!.Value);
        await Assert.That(storedMessage.Header.JobId!.Value).IsEquivalentTo(message.Header.JobId!.Value);

        // new fields assertions
        await Assert.That(storedMessage.Header.Source.ToString()).IsEquivalentTo(message.Header.Source.ToString());
        await Assert.That(storedMessage.Header.Type.Value).IsEquivalentTo(message.Header.Type.Value);
        await Assert.That(storedMessage.Header.DataSchema!.ToString()).IsEquivalentTo(message.Header.DataSchema?.ToString());
        await Assert.That(storedMessage.Header.Subject).IsEquivalentTo(message.Header.Subject!);
        await Assert.That(storedMessage.Header.TraceParent!.Value).IsEquivalentTo(message.Header.TraceParent?.Value);
        await Assert.That(storedMessage.Header.TraceState!.Value).IsEquivalentTo(message.Header.TraceState?.Value);
    }
    
    [Test]
    public virtual async Task When_Adding_A_Message_Within_Transaction_And_Rollback_It_Should_Not_Be_Stored()
    {
        // Arrange
        var transaction = CreateTransactionProvider();
        _ = transaction.GetTransaction();
        
        var context = new RequestContext();
        var message = CreateRandomMessage();
        
        // Act
        Outbox.Add(message, context, transactionProvider: transaction);
        transaction.Rollback();
        var storedMessage = Outbox.Get(message.Id, context);
        
        // Assert
        await Assert.That(storedMessage.Header.MessageType)
            .IsEqualTo(MessageType.MT_NONE);
    }
}