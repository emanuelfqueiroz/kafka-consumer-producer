using System;
using System.Collections.Generic;
using System.Text;

namespace Producer
{
    public class MessageSample
    {
        public string Id { get; set; }

        public MessageSample(string id)
        {
            Id = id;
        }
    }
}
