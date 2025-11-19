using Aliyun.OSS;
using Aliyun.OSS.Common;
using Aliyun.OSS.Common.Authentication;
using Paramore.Brighter.Transforms.Storage;

namespace Brighter.Transformers.Alibaba;

/// <summary>
/// Provides configuration options for Alibaba Cloud Object Storage Service (OSS) implementation 
/// of the Claim Check pattern in Brighter V10. This class configures how large messages are stored 
/// in OSS buckets when using the claim check pattern to avoid message size limitations.
/// </summary>
public class OssLuggageOptions(string endpoint, ICredentialsProvider credentialsProvider) : StorageOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OssLuggageOptions"/> class with the specified endpoint
    /// and credentials provider for Alibaba Cloud OSS.
    /// </summary>
    /// <param name="endpoint">The OSS service endpoint URL (e.g., "https://oss-cn-hangzhou.aliyuncs.com").</param>
    /// <param name="accessKeyId"></param>
    /// <param name="accessKeySecret"></param>
    public OssLuggageOptions(string endpoint, string accessKeyId, string accessKeySecret)
        : this(endpoint, new DefaultCredentialsProvider(new DefaultCredentials(accessKeyId, accessKeySecret, null)))
    {
        
    }

    /// <summary>
    /// Gets or sets the OSS service endpoint URL. This is the base URL for the OSS service in your region.
    /// </summary>
    /// <example>"https://oss-cn-hangzhou.aliyuncs.com"</example>
    /// <value>The OSS endpoint URL.</value>
    public string Endpoint { get; set; } = endpoint;

    /// <summary>
    /// Gets or sets the name of the OSS bucket where claim check data will be stored.
    /// </summary>
    /// <remarks>
    /// The bucket must exist before using this configuration. If not specified, defaults to "Brighter".
    /// Ensure the configured credentials have appropriate permissions to read/write to this bucket.
    /// </remarks>
    /// <value>The name of the OSS bucket.</value>
    public string BucketName { get; set; } = "Brighter";

    /// <summary>
    /// Gets or sets the prefix to apply to all object keys stored in the OSS bucket.
    /// </summary>
    /// <remarks>
    /// This allows logical organization of claim check objects within the bucket. For example, 
    /// setting prefix to "prod/" will store objects with keys like "prod/message-123".
    /// </remarks>
    /// <value>The prefix for object keys, or empty string for no prefix.</value>
    public string Prefix { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets or sets the storage class for objects stored in OSS.
    /// </summary>
    /// <remarks>
    /// Different storage classes offer different performance and cost characteristics:
    /// - Standard: For frequently accessed data
    /// - InfrequentAccess: For less frequently accessed data
    /// - Archive: For long-term archival storage
    /// Default is Standard storage class.
    /// </remarks>
    /// <value>The storage class for OSS objects.</value>
    public StorageClass StorageClass { get; set; } = StorageClass.Standard;

    /// <summary>
    /// Gets or sets the access control list (ACL) for objects stored in OSS.
    /// </summary>
    /// <remarks>
    /// Controls who can access the stored objects. Default is Private, meaning only the bucket owner 
    /// and authorized users can access the objects. Other options include PublicRead, PublicReadWrite, etc.
    /// </remarks>
    /// <value>The ACL policy for OSS objects.</value>
    public CannedAccessControlList ACL { get; set; } = CannedAccessControlList.Private;

    /// <summary>
    /// Gets or sets the data redundancy type for the OSS bucket.
    /// </summary>
    /// <remarks>
    /// Specifies the redundancy mechanism for object storage:
    /// - LRS (Local Redundancy Storage): Data is redundantly stored on multiple devices within a single zone
    /// - ZRS (Zone Redundancy Storage): Data is redundantly stored across multiple zones within the same region
    /// This is optional and can be null to use the bucket's default redundancy setting.
    /// </remarks>
    /// <value>The data redundancy type, or null to use bucket defaults.</value>
    public DataRedundancyType? DataRedundancyType { get; set; }
    
    /// <summary>
    /// Gets or sets the credentials provider used to authenticate with Alibaba Cloud OSS.
    /// </summary>
    /// <remarks>
    /// This provider supplies the access key ID, access key secret, and security token (if applicable) 
    /// for authenticating OSS operations. The default constructor provides a convenient way to create 
    /// a basic credentials provider from access keys.
    /// </remarks>
    /// <value>The credentials provider instance.</value>
    public ICredentialsProvider CredentialsProvider { get; set; } = credentialsProvider;
    
    /// <summary>
    /// Gets or sets the client configuration for the OSS client.
    /// </summary>
    /// <remarks>
    /// This allows customization of client behavior such as connection timeouts, retry policies, 
    /// proxy settings, and other network-related configurations. If not specified, default client 
    /// configuration will be used.
    /// </remarks>
    /// <value>The client configuration, or null to use defaults.</value>
    public ClientConfiguration? Configuration { get; set; }

    /// <summary>
    /// Creates and configures a new OSS client instance using the current configuration options.
    /// </summary>
    /// <returns>
    /// A fully configured <see cref="OssClient"/> instance ready to perform OSS operations 
    /// for the claim check pattern.
    /// </returns>
    /// <remarks>
    /// This method is typically used internally by Brighter's claim check infrastructure to 
    /// obtain the OSS client needed for storing and retrieving large message payloads.
    /// </remarks>
    public OssClient CreateStorageClient()
    {
        return new OssClient(Endpoint, CredentialsProvider, Configuration);
    }
}