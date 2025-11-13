using Paramore.Brighter;

namespace Brighter.Transformers.Alibaba.TestDoubles;

public class MyLargeCommand(int valueLength) : Command(Id.Random())
{
    public string Value { get; set; } = DataGenerator.CreateString(valueLength);

    public MyLargeCommand() : this(0)
    {
         /* requires a default constructor to deserialize*/
    }
}