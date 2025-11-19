using System;
using Aliyun.OTS;
using Paramore.Brighter.Observability;

namespace Brighter.Tablestore;

/// <summary>
/// Configuration class for Alibaba Cloud Table Store implementation of the Brighter inbox and outbox patterns.
/// </summary>
/// <remarks>
/// <para>
/// This class provides centralized configuration for the Brighter command processor's inbox, outbox, and locking mechanisms 
/// when using Alibaba Cloud Table Store as the persistence layer. It encapsulates all necessary settings for establishing 
/// connections, defining table structures, and configuring operational behavior.
/// </para>
/// <para>
/// The inbox pattern ensures reliable message processing by storing incoming messages before processing, while the outbox 
/// pattern guarantees message delivery by storing outgoing messages alongside business data changes. The locking mechanism 
/// provides distributed coordination for message processing across multiple instances.
/// </para>
/// <para>
/// <strong>Important:</strong> All table configurations (Inbox, Outbox, Locking) must be properly initialized before use. 
/// Failure to configure required tables will result in runtime exceptions during message processing.
/// </para>
/// </remarks>
public class TablestoreConfiguration
{
    /// <summary>
    /// Gets or sets the Alibaba Cloud Table Store client configuration used to establish connections to the service.
    /// </summary>
    /// <value>
    /// An instance of <see cref="OTSClientConfig"/> containing endpoint, authentication, and connection settings.
    /// This configuration is used to create the <see cref="OTSClient"/> instance for all Table Store operations.
    /// </value>
    /// <remarks>
    /// <para>
    /// The configuration must include valid endpoint URL, access credentials (AccessKey and AccessSecret), and the 
    /// Table Store instance name. These credentials should have appropriate permissions to read from and write to 
    /// the configured inbox, outbox, and locking tables.
    /// </para>
    /// <para>
    /// <strong>Security Note:</strong> Access credentials should be stored securely and not hard-coded in source code. 
    /// Consider using environment variables, configuration providers, or secret management services.
    /// </para>
    /// </remarks>
    public OTSClientConfig? Configuration { get; set; }

    /// <summary>
    /// Gets or sets the configuration for the inbox table used in the Brighter inbox pattern.
    /// </summary>
    /// <value>
    /// An instance of <see cref="TablestoreTable"/> defining the structure and properties of the inbox table.
    /// The inbox table stores incoming messages before they are processed by the command processor.
    /// </value>
    public TablestoreTable? Inbox { get; set; }
    
    /// <summary>
    /// Gets or sets the configuration for the outbox table used in the Brighter outbox pattern.
    /// </summary>
    /// <value>
    /// An instance of <see cref="TablestoreTable"/> defining the structure and properties of the outbox table.
    /// The outbox table stores outgoing messages that need to be published after business operations complete.
    /// </value>
    public TablestoreTable? Outbox { get; set; }
    
    /// <summary>
    /// Gets or sets the configuration for the locking table used for distributed coordination.
    /// </summary>
    /// <value>
    /// An instance of <see cref="TablestoreTable"/> defining the structure and properties of the locking table.
    /// The locking table provides distributed locks to coordinate message processing across multiple instances.
    /// </value>
    public TablestoreTable? Locking { get; set; }

    /// <summary>
    /// Gets or sets the name of the Alibaba Cloud Table Store database/instance.
    /// </summary>
    /// <value>
    /// The database name as a string. Defaults to an empty string.
    /// </value>
    /// <remarks>
    /// This property specifies the Table Store instance name where the inbox, outbox, and locking tables reside.
    /// It's used by Observability features to tag metrics and logs with the database context.
    /// </remarks>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the time provider used for timestamp operations within the inbox/outbox patterns.
    /// </summary>
    /// <value>
    /// An instance of <see cref="TimeProvider"/> used for generating timestamps. Defaults to <see cref="TimeProvider.System"/>.
    /// </value>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    
    /// <summary>
    /// Gets or sets the instrumentation options for monitoring and diagnostics.
    /// </summary>
    /// <value>
    /// An instance of <see cref="InstrumentationOptions"/> specifying which instrumentation features are enabled.
    /// Defaults to <see cref="InstrumentationOptions.All"/>, enabling all available instrumentation.
    /// </value>
    public InstrumentationOptions Instrumentation { get; set; } = InstrumentationOptions.All;
}