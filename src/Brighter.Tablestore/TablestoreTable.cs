using System;

namespace Brighter.Tablestore;

public class TablestoreTable
{
    public string Name { get; set; } = string.Empty;
    public TimeSpan? TimeToLive { get; set; }
}