using Brighter.Transformers.Alibaba.TestDoubles;
using Paramore.Brighter;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Transforms.Transformers;

namespace Brighter.Transformers.Alibaba.Transformers;

public class LargeMessagePayloadWrapTests : IDisposable 
{
    private string? _id;
    private readonly string _bucketName;
    private readonly TransformPipelineBuilderAsync _pipelineBuilder;
    private readonly MyLargeCommand _myCommand;
    private readonly OssLuggageOptions _luggageStoreOptions;
    private readonly OssLuggageStore _luggageStore;
    private readonly Publication _publication;

    public LargeMessagePayloadWrapTests()
    {
        //arrange
        TransformPipelineBuilderAsync.ClearPipelineCache();
            
        var mapperRegistry =
            new MessageMapperRegistry(null, new SimpleMessageMapperFactoryAsync(
                _ => new MyLargeCommandMessageMapperAsync())
            );
           
        mapperRegistry.RegisterAsync<MyLargeCommand, MyLargeCommandMessageMapperAsync>();
            
        _myCommand = new MyLargeCommand(6000);

        _bucketName = $"brightertestbucket-{Guid.NewGuid()}";
        _luggageStoreOptions = new OssLuggageOptions(
            AlibabaConfiguration.OssEndpoint,
            AlibabaConfiguration.AccessKey, 
            AlibabaConfiguration.SecretKey)
        {
            BucketName = _bucketName
        };
        
        _luggageStore = new OssLuggageStore(_luggageStoreOptions);
        _luggageStore.EnsureStoreExists();

        var transformerFactoryAsync = new SimpleMessageTransformerFactoryAsync(_ => new ClaimCheckTransformer(_luggageStore, _luggageStore));

        _publication = new Publication { Topic = new RoutingKey("MyLargeCommand"), RequestType = typeof(MyLargeCommand) };

        _pipelineBuilder = new TransformPipelineBuilderAsync(mapperRegistry, transformerFactoryAsync, InstrumentationOptions.None);
    }

    [Test]
    public async Task When_wrapping_a_large_message()
    {
        //act
        var transformPipeline = _pipelineBuilder.BuildWrapPipeline<MyLargeCommand>();
        var message = await transformPipeline.WrapAsync(_myCommand, new RequestContext(), _publication);

        //assert
        await Assert.That(message.Header.Bag).ContainsKey(ClaimCheckTransformer.CLAIM_CHECK);
        Assert.NotNull(message.Header.DataRef);

        _id = (string)message.Header.Bag[ClaimCheckTransformer.CLAIM_CHECK];
        await Assert.That(message.Body.Value).IsEqualTo($"Claim Check {_id}");

        await Assert.That(() => _luggageStore.HasClaimAsync(_id))
            .IsTrue();
    }

    public void Dispose()
    {
         //We have to empty objects from a bucket before deleting it
         if (_id != null)
         {
             _luggageStore.Delete(_id);
         }

         var client = _luggageStoreOptions.CreateStorageClient();
         client.DeleteBucket(_bucketName);
    }
}
