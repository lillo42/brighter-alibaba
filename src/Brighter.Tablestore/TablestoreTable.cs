namespace Brighter.Tablestore;

/// <summary>
/// Represents the configuration for an Alibaba Cloud Table Store table used in the Brighter inbox and outbox pattern implementation.
/// </summary>
/// <remarks>
/// <para>
/// This class defines the table configuration needed for storing messages in Alibaba Cloud Table Store as part of the 
/// Brighter command processor's inbox and outbox patterns. Each table (inbox, outbox, locking) requires a name and 
/// optionally an index name for efficient querying.
/// </para>
/// </remarks>
public class TablestoreTable
{
    /// <summary>
    /// Gets or sets the name of the Alibaba Cloud Table Store table.
    /// </summary>
    /// <value>
    /// The name of the table in Alibaba Cloud Table Store. This must match an existing table name 
    /// that has been created with the appropriate schema for message storage.
    /// </value>
    /// <remarks>
    /// <para>
    /// The table name must conform to Alibaba Cloud Table Store naming conventions:
    /// - Must be 1-255 characters long
    /// - Can only contain letters, numbers, underscores (_), and hyphens (-)
    /// - Must start with a letter or underscore
    /// </para>
    /// <para>
    /// For the Brighter pattern implementation, you typically need three tables:
    /// - Inbox table: stores incoming messages awaiting processing
    /// - Outbox table: stores outgoing messages awaiting dispatch
    /// - Locking table: provides distributed locking for message coordination
    /// </para>
    /// <para>
    /// The table schema should include appropriate primary keys and attributes for message storage. 
    /// Common attributes include MessageId, MessageType, MessageBody, Timestamp, Dispatched status, etc.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// var tableConfig = new TablestoreTable
    /// {
    ///     Name = "brighter_messages_outbox" // Valid table name
    /// };
    /// </code>
    /// </example>
    public string Name { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets or sets the name of the search index used for efficient querying of the outbox table.
    /// </summary>
    /// <value>
    /// The name of the search index in Alibaba Cloud Table Store, or <c>null</c> if we are going to use the default one.
    /// </value>
    public string? IndexName { get; set; }
}