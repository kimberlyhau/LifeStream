using System;

namespace Streamer.Ingest
{
    public struct Signal : IComparable<Signal>
    {
        public long ts;
        public float val;
        public float [] vals;

        public Signal(long ts, float val, float [] vals)
        {
            this.ts = ts;
            this.val = val;
            //getter and setter? define for all fields
            this.vals = vals; //copy val to vals, change size of array
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