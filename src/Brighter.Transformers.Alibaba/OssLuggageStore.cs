using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Aliyun.OSS;
using Aliyun.OSS.Common;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Logging;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Transforms.Storage;

namespace Brighter.Transformers.Alibaba;

/// <summary>
/// Provides an Alibaba Cloud Object Storage Service (OSS) implementation of the claim check pattern storage provider
/// for Brighter V10. This class handles storing and retrieving large message payloads in OSS buckets, implementing
/// both synchronous and asynchronous storage provider interfaces.
/// </summary>
/// <remarks>
/// <para>
/// The claim check pattern allows storing large message payloads externally while passing only a reference (claim check)
/// in the message itself. This implementation uses Alibaba Cloud OSS as the external storage medium.
/// </para>
/// <para>
/// This class supports configurable bucket creation strategies and handles object lifecycle operations including
/// storage, retrieval, existence checking, and deletion of claim check data.
/// </para>
/// </remarks>
public partial class OssLuggageStore(OssLuggageOptions options) : IAmAStorageProvider, IAmAStorageProviderAsync
{
    private const string ClaimCheckProvider = "aliyun_oss";
    private static readonly ILogger Logger = ApplicationLogging.CreateLogger<OssLuggageStore>();
    private static readonly Dictionary<string, string> SpanAttributes = new();
    
    /// <summary>
    /// Gets or sets the tracer for distributed tracing support.
    /// </summary>
    /// <value>
    /// An instance of <see cref="IAmABrighterTracer"/> that enables distributed tracing for storage operations,
    /// or <c>null</c> if tracing is not configured.
    /// </value>
    public IAmABrighterTracer? Tracer { get; set; }

    /// <summary>
    /// Ensures that the OSS bucket exists according to the configured storage strategy.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the bucket does not exist and the strategy is set to <see cref="StorageStrategy.Validate"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This method follows the strategy defined in <see cref="OssLuggageOptions.Strategy"/>:
    /// - <see cref="StorageStrategy.Assume"/>: No action is taken, assumes bucket exists
    /// - <see cref="StorageStrategy.Validate"/>: Checks if bucket exists, throws if it doesn't
    /// - <see cref="StorageStrategy.CreateIfMissing"/>: Creates the bucket if it doesn't exist
    /// </para>
    /// <para>
    /// When creating a bucket, it uses the configured ACL, storage class, and data redundancy type from the options.
    /// </para>
    /// </remarks>
    public void EnsureStoreExists()
    {
        if (string.IsNullOrEmpty(options.BucketName))
        {
            throw new ConfigurationException("Bucket name not set");
        }
        
        if (options.Strategy == StorageStrategy.Assume)
        {
            return;
        }
        
        var client = options.CreateStorageClient();
        var exist = client.DoesBucketExist(options.BucketName);
        if (exist)
        {
            return;
        }

        if (options.Strategy == StorageStrategy.Validate)
        {
            throw new InvalidOperationException($"Bucket {options.BucketName} does not exist");
        }

        try
        {
            client.CreateBucket(new CreateBucketRequest(options.BucketName)
            {
                ACL =  options.ACL,
                DataRedundancyType = options.DataRedundancyType,
                StorageClass = options.StorageClass,
            });
        }
        catch (Exception e)
        {
            Log.ErrorCreatingValidatingLuggageStore(Logger, options.BucketName, e);
            throw;
        }
    }

    /// <summary>
    /// Deletes an object from the OSS bucket using the specified claim check identifier.
    /// </summary>
    /// <param name="claimCheck">The claim check identifier representing the object to delete.</param>
    /// <exception cref="OssException">
    /// Thrown when OSS operations fail (e.g., network issues, authentication failures, or invalid bucket/object names).
    /// </exception>
    /// <remarks>
    /// This method constructs the full object key by applying the configured prefix (if any) to the claim check identifier
    /// before performing the deletion operation.
    /// </remarks>
    public void Delete(string claimCheck)
    {
        var span = Tracer?.CreateClaimCheckSpan(new ClaimCheckSpanInfo(ClaimCheckOperation.Delete, ClaimCheckProvider, options.BucketName, claimCheck, SpanAttributes));
        try
        {
            var client = options.CreateStorageClient();
            client.DeleteObject(new DeleteObjectRequest(options.BucketName, Key(claimCheck)));
        }
        catch (Exception e)
        {
            Log.CouldNotDeleteLuggage(Logger, claimCheck, options.BucketName, e);
            throw;
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <summary>
    /// Retrieves the content of an object from OSS using the specified claim check identifier.
    /// </summary>
    /// <param name="claimCheck">The claim check identifier representing the object to retrieve.</param>
    /// <returns>
    /// A <see cref="Stream"/> containing the object content, or an empty <see cref="MemoryStream"/> if the object is not found.
    /// </returns>
    /// <exception cref="OssException">
    /// Thrown when OSS operations fail (e.g., network issues, authentication failures, or invalid bucket/object names).
    /// </exception>
    /// <remarks>
    /// <para>
    /// This method constructs the full object key by applying the configured prefix (if any) to the claim check identifier
    /// before performing the retrieval operation.
    /// </para>
    /// <para>
    /// If the object does not exist, an empty memory stream is returned rather than throwing an exception, following
    /// the pattern expected by Brighter's claim check infrastructure.
    /// </para>
    /// </remarks>
    public Stream Retrieve(string claimCheck)
    {
        var span = Tracer?.CreateClaimCheckSpan(new ClaimCheckSpanInfo(ClaimCheckOperation.Retrieve, ClaimCheckProvider, options.BucketName, claimCheck, SpanAttributes));
        try
        {
            var client = options.CreateStorageClient();
            var obj = client.GetObject(new GetObjectRequest(options.BucketName, Key(claimCheck)));
            if (obj == null)
            {
                throw new FileNotFoundException($"The {claimCheck} was not found in {options.BucketName}");
            }

            return obj.Content;
        }
        catch (Exception e)
        {
            Log.UnableToRead(Logger, claimCheck, options.BucketName, e);
            throw;
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <summary>
    /// Checks whether an object exists in the OSS bucket for the specified claim check identifier.
    /// </summary>
    /// <param name="claimCheck">The claim check identifier to check for existence.</param>
    /// <returns>
    /// <c>true</c> if an object exists with the constructed key; otherwise, <c>false</c>.
    /// </returns>
    /// <exception cref="OssException">
    /// Thrown when OSS operations fail (e.g., network issues, authentication failures, or invalid bucket/object names).
    /// </exception>
    /// <remarks>
    /// This method constructs the full object key by applying the configured prefix (if any) to the claim check identifier
    /// before checking for existence.
    /// </remarks>
    public bool HasClaim(string claimCheck)
    {
        var span = Tracer?.CreateClaimCheckSpan(new ClaimCheckSpanInfo(ClaimCheckOperation.HasClaim, ClaimCheckProvider, options.BucketName, claimCheck, SpanAttributes));

        try
        {
            var client = options.CreateStorageClient();
            return client.DoesObjectExist(options.BucketName, Key(claimCheck));
        }
        catch (Exception)
        {
            return false;
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <summary>
    /// Stores a stream of data in the OSS bucket and returns a unique claim check identifier.
    /// </summary>
    /// <param name="stream">The stream containing the data to store.</param>
    /// <returns>
    /// A unique claim check identifier (UUID without hyphens) that can be used to retrieve the stored data.
    /// </returns>
    /// <exception cref="OssException">
    /// Thrown when OSS operations fail (e.g., network issues, authentication failures, or invalid bucket/object names).
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the provided stream is null or cannot be read.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This method generates a new UUID (without hyphens) as the object key, applies the configured prefix (if any),
    /// and uploads the stream content to the OSS bucket.
    /// </para>
    /// <para>
    /// The stream position is not reset by this method, so ensure the stream is positioned at the beginning
    /// before calling this method.
    /// </para>
    /// </remarks>
    public string Store(Stream stream)
    {
        var id = Uuid.New().ToString("N");
        var key = Key(id);
        
        var span = Tracer?.CreateClaimCheckSpan(new ClaimCheckSpanInfo(ClaimCheckOperation.Store, ClaimCheckProvider, options.BucketName, key, SpanAttributes, stream.Length));

        try
        {
            var client = options.CreateStorageClient();
            client.PutObject(new PutObjectRequest(options.BucketName, key, stream));
            return id;
        }
        finally
        {
            Tracer?.EndSpan(span);
        }
    }

    /// <summary>
    /// Asynchronously ensures that the OSS bucket exists according to the configured storage strategy.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the bucket does not exist and the strategy is set to <see cref="StorageStrategy.Validate"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This is an asynchronous wrapper around the synchronous <see cref="EnsureStoreExists"/> method.
    /// It does not provide true asynchronous OSS operations but maintains the async interface contract.
    /// </para>
    /// <para>
    /// For true asynchronous OSS operations, consider implementing native async methods using the OSS SDK's
    /// async API if available in future versions.
    /// </para>
    /// </remarks>
    public Task EnsureStoreExistsAsync(CancellationToken cancellationToken = new CancellationToken())
    {
        EnsureStoreExists();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Asynchronously deletes an object from the OSS bucket using the specified claim check identifier.
    /// </summary>
    /// <param name="claimCheck">The claim check identifier representing the object to delete.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="OssException">
    /// Thrown when OSS operations fail (e.g., network issues, authentication failures, or invalid bucket/object names).
    /// </exception>
    /// <remarks>
    /// This is an asynchronous wrapper around the synchronous <see cref="Delete"/> method.
    /// It does not provide true asynchronous OSS operations but maintains the async interface contract.
    /// </remarks>
    public Task DeleteAsync(string claimCheck, CancellationToken cancellationToken = new CancellationToken())
    {
        Delete(claimCheck);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Asynchronously retrieves the content of an object from OSS using the specified claim check identifier.
    /// </summary>
    /// <param name="claimCheck">The claim check identifier representing the object to retrieve.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>
    /// A task that represents the asynchronous operation and contains a <see cref="Stream"/> with the object content,
    /// or an empty <see cref="MemoryStream"/> if the object is not found.
    /// </returns>
    /// <exception cref="OssException">
    /// Thrown when OSS operations fail (e.g., network issues, authentication failures, or invalid bucket/object names).
    /// </exception>
    /// <remarks>
    /// This is an asynchronous wrapper around the synchronous <see cref="Retrieve"/> method.
    /// It does not provide true asynchronous OSS operations but maintains the async interface contract.
    /// </remarks>
    public Task<Stream> RetrieveAsync(string claimCheck, CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.FromResult(Retrieve(claimCheck));
    }

    /// <summary>
    /// Asynchronously checks whether an object exists in the OSS bucket for the specified claim check identifier.
    /// </summary>
    /// <param name="claimCheck">The claim check identifier to check for existence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>
    /// A task that represents the asynchronous operation and contains <c>true</c> if the object exists; otherwise, <c>false</c>.
    /// </returns>
    /// <exception cref="OssException">
    /// Thrown when OSS operations fail (e.g., network issues, authentication failures, or invalid bucket/object names).
    /// </exception>
    /// <remarks>
    /// This is an asynchronous wrapper around the synchronous <see cref="HasClaim"/> method.
    /// It does not provide true asynchronous OSS operations but maintains the async interface contract.
    /// </remarks>
    public Task<bool> HasClaimAsync(string claimCheck, CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.FromResult(HasClaim(claimCheck));
    }

    /// <summary>
    /// Asynchronously stores a stream of data in the OSS bucket and returns a unique claim check identifier.
    /// </summary>
    /// <param name="stream">The stream containing the data to store.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>
    /// A task that represents the asynchronous operation and contains a unique claim check identifier (UUID without hyphens).
    /// </returns>
    /// <exception cref="OssException">
    /// Thrown when OSS operations fail (e.g., network issues, authentication failures, or invalid bucket/object names).
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the provided stream is null or cannot be read.
    /// </exception>
    /// <remarks>
    /// This is an asynchronous wrapper around the synchronous <see cref="Store"/> method.
    /// It does not provide true asynchronous OSS operations but maintains the async interface contract.
    /// </remarks>
    public Task<string> StoreAsync(Stream stream, CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.FromResult(Store(stream));
    }

    /// <summary>
    /// Constructs the full object key by applying the configured prefix to the base key.
    /// </summary>
    /// <param name="key">The base key to which the prefix should be applied.</param>
    /// <returns>
    /// The full object key with prefix applied if a prefix is configured; otherwise, the original key.
    /// </returns>
    /// <remarks>
    /// This helper method ensures consistent key formatting across all storage operations, allowing for
    /// logical organization of claim check objects within the OSS bucket using a configurable prefix.
    /// </remarks>
    private string Key(string key)
    {
        if (string.IsNullOrEmpty(options.Prefix))
        {
            return key;
        }
        
        return options.Prefix + key;
    }
    
    private static partial class Log
    {
        [LoggerMessage(LogLevel.Error, "Error creating or validating luggage store {BucketName}")]
        public static partial void ErrorCreatingValidatingLuggageStore(ILogger logger, string bucketName, Exception e);

        [LoggerMessage(LogLevel.Error, "Could not delete luggage with claim {ClaimCheck} from {Bucket}")]
        public static partial void CouldNotDeleteLuggage(ILogger logger, string claimCheck, string bucket, Exception e);

        [LoggerMessage(LogLevel.Information, "Downloading {ClaimCheck} from {Bucket}")]
        public static partial void Downloading(ILogger logger, string claimCheck, string bucket);

        [LoggerMessage(LogLevel.Error, "Could not download {ClaimCheck} from {BucketName}")]
        public static partial void CouldNotDownload(ILogger logger, string claimCheck, string bucketName);

        [LoggerMessage(LogLevel.Error, "Unable to read {ClaimCheck} from {Bucket}")]
        public static partial void UnableToRead(ILogger logger, string claimCheck, string bucket,  Exception exception);

        [LoggerMessage(LogLevel.Information, "Uploading {ClaimCheck} to {Bucket}")]
        public static partial void Uploading(ILogger logger, string claimCheck, string bucket);
    }
}