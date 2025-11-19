namespace Brighter.Tablestore;

/// <summary>
/// Represents a no-op transaction for Tablestore operations.
/// Tablestore does not support traditional transactions, so this class serves as a placeholder
/// to satisfy the transaction provider interface contract.
/// </summary>
public class NoTransaction;