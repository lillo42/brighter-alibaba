using Aliyun.OTS;
using Brighter.Outbox.Tablestore;
using Brighter.Tablestore;
using Paramore.Brighter;

namespace Brighter.Transformers.Alibaba.Outbox;

[InheritsTests]
public class TablestoreOutboxAsyncTest : OutboxAsyncTest<NoTransaction>
{
    private TablestoreOutbox? _outbox;
    protected override IAmAnOutboxAsync<Message, NoTransaction> Outbox => _outbox!;

    public override Task CreateStoreAsync()
    {
        _outbox = new TablestoreOutbox(new TablestoreConfiguration
        {
            Configuration = new OTSClientConfig(
                AlibabaConfiguration.TablestoreEndpoint,
                AlibabaConfiguration.AccessKey,
                AlibabaConfiguration.SecretKey,
                "brighter"),
            DatabaseName = "brighter",
            Outbox = new TablestoreTable
            {
                Name = "outbox"
            }
        });

        return Task.CompletedTask;
    }

    public override async Task DeleteStoreAsync()
    {
        try
        {
            await _outbox!.DeleteAsync(
                CreatedMessages.Select(x => x.Id).ToArray(), 
                new RequestContext());
        }
        catch (Exception)
        {
        }
    }

    protected override async Task<IEnumerable<Message>> GetAllMessagesAsync()
    {
        return await _outbox!.GetAsync(pageSize: 99);
    }

    protected override Task<IAmABoxTransactionProvider<NoTransaction>> CreateTransactionProviderAsync()
    {
        return Task.FromResult<IAmABoxTransactionProvider<NoTransaction>>(new TablestoreConnectionProvider(new TablestoreConfiguration
        {
            Configuration = new OTSClientConfig(
                AlibabaConfiguration.TablestoreEndpoint,
                AlibabaConfiguration.AccessKey,
                AlibabaConfiguration.SecretKey,
                "brighter")
        }));
    }

    public override Task When_Adding_A_Message_Within_Transaction_And_Rollback_It_Should_Not_Be_Stored()
    {
        return Task.CompletedTask;
    }
}