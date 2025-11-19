using Aliyun.OTS;
using Paramore.Brighter;

namespace Brighter.Tablestore;

/// <summary>
/// Provides connection and transaction management for implementing the Brighter inbox and outbox patterns with Alibaba Cloud Table Store.
/// </summary>
/// <remarks>
/// This interface extends <see cref="IAmAConnectionProvider"/> for connection management and <see cref="IAmABoxTransactionProvider{NoTransaction}"/>
/// for transaction handling with no transaction support, specifically tailored for Alibaba Cloud Table Store integration.
/// The outbox pattern implemented through this provider ensures at-least-once message delivery while maintaining transactional consistency
/// between database updates and message publishing.
/// </remarks>
public interface IAmATablestoreConnectionProvider : IAmAConnectionProvider, IAmABoxTransactionProvider<NoTransaction>
{
    /// <summary>
    /// Provides connection and transaction management for implementing the Brighter inbox and outbox patterns with Alibaba Cloud Table Store.
    /// </summary>
    /// <remarks>
    /// This interface extends <see cref="IAmAConnectionProvider"/> for connection management and <see cref="IAmABoxTransactionProvider{NoTransaction}"/>
    /// for transaction handling with no transaction support, specifically tailored for Alibaba Cloud Table Store integration.
    /// The outbox pattern implemented through this provider ensures at-least-once message delivery while maintaining transactional consistency
    /// between database updates and message publishing.
    /// </remarks>
    OTSClient GetTablestoreClient();
}