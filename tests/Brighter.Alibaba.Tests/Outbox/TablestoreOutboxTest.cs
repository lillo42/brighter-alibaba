using Aliyun.OTS;
using Brighter.Outbox.Tablestore;
using Brighter.Tablestore;
using Paramore.Brighter;

namespace Brighter.Transformers.Alibaba.Outbox;

[InheritsTests]
public class TablestoreOutboxTest : OutboxTest<NoTransaction>
{
    private TablestoreOutbox? _outbox;
    protected override IAmAnOutboxSync<Message, NoTransaction> Outbox => _outbox!;

    public override void CreateStore()
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
    }

    public override void DeleteStore()
    {
        try
        {
            _outbox!.Delete(
                CreatedMessages.Select(x => x.Id).ToArray(), 
                new RequestContext());
        }
        catch (Exception)
        {
        }
    }

    protected override IEnumerable<Message> GetAllMessages()
    {
        return _outbox!.Get(pageSize: 99);
    }

    protected override IAmABoxTransactionProvider<NoTransaction> CreateTransactionProvider()
    {
        return new TablestoreConnectionProvider(new TablestoreConfiguration
        {
            Configuration = new OTSClientConfig(
                AlibabaConfiguration.TablestoreEndpoint,
                AlibabaConfiguration.AccessKey,
                AlibabaConfiguration.SecretKey,
                "brighter")
        });
    }

    public override Task When_Adding_A_Message_Within_Transaction_And_Rollback_It_Should_Not_Be_Stored()
    {
        return Task.CompletedTask;
    }
}