using Paramore.Brighter.Transforms.Storage;

namespace Brighter.Transformers.Alibaba.Transformers;

public class LuggageStoreExistsTests 
{
    [Test]
    public async Task When_checking_store_that_exists()
    {
        var bucketName = $"brightertestbucket-{Guid.NewGuid()}";
        var options = new OssLuggageOptions(AlibabaConfiguration.Endpoint,
            AlibabaConfiguration.AccessKey, AlibabaConfiguration.SecretKey)
        {
            BucketName = bucketName
        };
        
        var luggageStore = new OssLuggageStore(options);
        await luggageStore.EnsureStoreExistsAsync();
        
        // act
        options.Strategy = StorageStrategy.Validate;
        await luggageStore.EnsureStoreExistsAsync();

        //teardown
        var client = options.CreateStorageClient();
        client.DeleteBucket(bucketName);
    }
    
    [Test]
    public async Task When_checking_store_that_does_not_exist()
    {
        //act
         var doesNotExist = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
             {
                 var bucketName = $"brightertestbucket-{Guid.NewGuid()}";
                 var options = new OssLuggageOptions(AlibabaConfiguration.Endpoint,
                    AlibabaConfiguration.AccessKey, AlibabaConfiguration.SecretKey)
                 { 
                     BucketName = bucketName 
                 };
        
                 var luggageStore = new OssLuggageStore(options); 
                 await luggageStore.EnsureStoreExistsAsync();
             });
         Assert.NotNull(doesNotExist);
    }
}
