using Brighter.Transformers.Alibaba.TestDoubles;
using Paramore.Brighter;
using Paramore.Brighter.Inbox.Exceptions;

namespace Brighter.Transformers.Alibaba.Inbox;

public abstract class InboxAsyncTests
{
    protected abstract IAmAnInboxAsync Inbox { get; }
    protected virtual List<MyCommand> CreatedCommands { get; } = [];

    [Before(Test)]
    public virtual async Task BeforeEachTestAsync()
    {
        await CreateStoreAsync();
    }
    
    protected virtual Task CreateStoreAsync()
    {
        return Task.CompletedTask;
    }
    
    [After(Test)]
    public virtual async Task AfterEachTestAsync()
    {
        await DeleteStoreAsync();
    }

    protected virtual Task DeleteStoreAsync()
    {
        return Task.CompletedTask;
    }
    
    protected virtual MyCommand CreateCommand()
    {
        var command = new MyCommand { Value = Uuid.NewAsString() };
        
        CreatedCommands.Add(command);

        return command;
    }

    [Test]
    public async Task When_Adding_A_Command_To_The_Inbox_It_Can_Be_Retrieved()
    {
        // Arrange
        var contextKey = Uuid.NewAsString();
        var command = CreateCommand();
        
        // Act 
        await Inbox.AddAsync(command, contextKey, null);
        var loadedCommand = await Inbox.GetAsync<MyCommand>(command.Id, contextKey, null);
        
        // Assert
        Assert.NotNull(loadedCommand);
        await Assert.That(loadedCommand).IsEquivalentTo(command);
    }
    
    [Test]
    public async Task When_Adding_A_Duplicate_Command_With_Same_Context_Key_It_Should_Not_Throw()
    {
        // Arrange
        var contextKey = Uuid.NewAsString();
        var command = CreateCommand();
        
        await Inbox.AddAsync(command, contextKey, null);

        // Act 
        await Inbox.AddAsync(command, contextKey, null);
        
        // Assert
        await Assert.That(Inbox.ExistsAsync<MyCommand>(command.Id, contextKey, null))
            .IsTrue();
    }
    
    [Test]
    public async Task When_Adding_A_Duplicate_Command_With_Different_Context_Key_It_Should_Not_Throw()
    {
        // Arrange
        var contextKey = Uuid.NewAsString();
        var command = CreateCommand();
        
        await Inbox.AddAsync(command, contextKey, null);

        // Act 
        await Inbox.AddAsync(command, Uuid.NewAsString(), null);
        
        // Assert
        await Assert.That(Inbox.ExistsAsync<MyCommand>(command.Id, contextKey, null))
            .IsTrue();
    }
    
    [Test]
    public async Task When_Getting_A_Non_Existent_Command_It_Should_Throw_RequestNotFoundException()
    {
        // Arrange
        var contextKey = Uuid.NewAsString();
        var commandId = Uuid.NewAsString();
        
        // Act & Assert
        await Assert.ThrowsAsync<RequestNotFoundException<MyCommand>>(() => Inbox.GetAsync<MyCommand>(commandId, contextKey, null));
    }
    
    [Test]
    public async Task When_Getting_A_Command_With_Wrong_Context_Key_It_Should_Throw_RequestNotFoundException()
    {
        // Arrange
        var command = CreateCommand();
        await Inbox.AddAsync(command, Uuid.NewAsString(), null);
        
        // Act & Assert
        await Assert.ThrowsAsync<RequestNotFoundException<MyCommand>>(() => Inbox.GetAsync<MyCommand>(command.Id, Uuid.NewAsString(), null));
    }

    [Test]
    public async Task When_Checking_If_A_Non_Existent_Command_Exists_It_Should_Return_False()
    {
       await Assert.That(Inbox.ExistsAsync<MyCommand>(Uuid.NewAsString(), Uuid.NewAsString(), null))
           .IsFalse();
    }
}