using System;
using System.Windows.Markup;

namespace Streamer.Ingest
{
    public struct Signal : IComparable<Signal>
    {
        public long ts;
        public float val;
        public float val2;
        public float val3;
        public float val4;
        public float val5;
        

        public Signal(long ts, float val)
        {
            this.ts = ts;
            this.val = val;
            this.val2 = val;
            this.val3 = val;
            this.val4 = val;
            this.val5 = val;
            
        }

        public int CompareTo(Signal other)
        {
            return (int) (this.ts - other.ts);
        }

        public override string ToString()
        {
            return $"ts={this.ts}, val={this.val}";
        }
    }
}