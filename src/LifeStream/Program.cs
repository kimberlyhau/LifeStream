using System;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.FOperationAPI;
using Streamer.Ingest;
using System.Collections.Generic;
using System.Linq;

namespace LifeStream
{
    class Program
    {
        
        static double NonFuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<IStreamable<Empty, Signal>, IStreamable<Empty, TResult>> transform)
        {
            var stream = data();

            var sw = new Stopwatch();
            sw.Start();
            var s_obs = transform(stream);

            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static double FuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<FOperation<Signal>, FOperation<TResult>> transform)
        {
            var stream = data();

            var sw = new Stopwatch();
            sw.Start();
            var fStart = stream
                    .FuseStart()
                ;

            var s_obs = transform(fStart.GetFOP())
                    .FuseEnd()
                ;

            fStart.Connect();
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static double MultiNonFuseTest(
            Func<IStreamable<Empty, Signal>> abp_data, Func<IStreamable<Empty, Signal>> ecg_data)
        {
            var abp = abp_data();
            var ecg = ecg_data();

            var sw = new Stopwatch();
            sw.Start();
            var window = 60000;
            var gap_tol = 60000;
            var fstream1 = abp
                    .FillMean(window, 8, gap_tol)
                    .Resample(8, 2)
                    .Normalize(window)
                ;
            var fstream2 = ecg
                    .FillMean(window, 2, gap_tol)
                    .Normalize(window)
                ;

            var s_obs = fstream2
                    .Join(fstream1, (l, r) => new {l, r})
                ;
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        internal static void PairJoiner(Signal l, Signal r, out FStreamable.SigPair o)
        {
            o.s = l;
            o.e = r;
        }
        
        static double MultiFuseTest(Func<IStreamable<Empty, Signal>> abp_data,
            Func<IStreamable<Empty, Signal>> ecg_data)
        {
            var abp = abp_data();
            var ecg = ecg_data();

            var sw = new Stopwatch();
            sw.Start();
            var fabp = abp
                    .FuseStart()
                ;
            var fecg = ecg
                    .FuseStart()
                ;

            var window = 60000;
            var gap_tol = 60000;
            var fstream1 = fabp.GetFOP()
                    .FillMean(window, 8, gap_tol)
                    .Resample(8, 2, 8)
                    .Normalize(2, window)
                ;
            var fstream2 = fecg.GetFOP()
                    .FillMean(window, 2, gap_tol)
                    .Normalize(2, window)
                ;

            var s_obs = fstream2
                    .Join<Signal, Signal, FStreamable.SigPair>(fstream1, PairJoiner)
                    .FuseEnd()
                ;

            fabp.Connect();
            fecg.Connect();
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }
        
        public struct TwoLongs
        {
            public long a;
            public long b;
        }

        static void ChangingTypes()
        {
            Config.DataBatchSize = 120000;
            Config.FuseFactor = 1;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.ForceRowBasedExecution = true;


            //char
            /*
            var listA = new List<char>();
            for (int j = 0; j < 500; j++)
            {
                for (int i = 0; i < 60000; i++)
                {
                    listA.Add((char)i);
                }
            }

            var data = new {ts = 0, p = listA[0]};
            var list = new[] {data}.ToList();
            for (int i = 1; i < listA.Count; i++)
            {
                data = new {ts =i, p=listA[i]};
                list.Add(data);
            }

            var streamA = list
                    .ToObservable()
                    .ToTemporalStreamable(e => e.ts, e => e.ts + 1)
                    .Select(e => e.p)
                    .Cache()
                ;
            */
            //int
            //var listA = new List<int>();   
            //long
            //var listA = new List<long>();  
            /*
            for (int i = 0; i < 30; i++)
            {
                listA.Add(i);                               
            }
            
            var streamA = listA                 
                    .ToObservable()                         
                    .ToTemporalStreamable(e => e, e => e + 1)  
                    .Cache()
                ; 
            */
            
            //two longs
            
            var listA = new List<TwoLongs>();                       
            for (int i = 0; i < 300; i++)
            {
                TwoLongs p;
                
                p.a = i;
                p.b = i;
                listA.Add(p);                               
            }
            
            var streamA = listA                 
                    .ToObservable()                         
                    .ToTemporalStreamable(e => e.a, e => (e.a + 1))  
                    .Cache()
                ; 
            
            //ops
            /*
            var sw = new Stopwatch();
            sw.Start();
            var s_obs = streamA
                .Select(e => e + 1);

            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            
            Console.WriteLine("Op:Select,  Time: {0:.###} sec",
                sw.Elapsed.TotalSeconds);
            
            var sw2 = new Stopwatch();
            sw2.Start();
            var s_obs2 = streamA
                .Where(e => e == 0);

            s_obs2
                .ToStreamEventObservable()
                .Wait();
            sw2.Stop();
            
            Console.WriteLine("Op:Where,  Time: {0:.###} sec",
                sw2.Elapsed.TotalSeconds);
            Config.StreamScheduler.Stop();
            
            var sw3 = new Stopwatch();
            sw3.Start();
            var s_obs3 = streamA
                .Join(streamA, (l,r)=> l);

            s_obs3
                .ToStreamEventObservable()
                .Wait();
            sw3.Stop();
            
            Console.WriteLine("Op:Join,  Time: {0:.###} sec",
                sw3.Elapsed.TotalSeconds);
            */
            var sw4 = new Stopwatch();
            sw4.Start();
            var s_obs4 = streamA
                .TumblingWindowLifetime(10)
                .Aggregate(w=> w.Count());

            s_obs4
                .ToStreamEventObservable()
                .Wait();
            sw4.Stop();
            s_obs4
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            Console.WriteLine("Op:Aggregate,  Time: {0:.###} sec",
                sw4.Elapsed.TotalSeconds);
            
            Config.StreamScheduler.Stop();
        }

        static void BenchMarks(int dur, string eng , string test )
        {
            Config.DataBatchSize = 120000;
            Config.FuseFactor = 1;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.ForceRowBasedExecution = true;

            int duration = dur;
            var testcase = test; //normalize, passfilter, fillconst, fillmean, resample, endtoend
            var engine = eng;
            double time = 0;

            const int start = 0;
            const int freq = 500;
            const int period = 1000 / freq;
            const long window = 60000;
            const long gap_tol = window;
            long count = (duration * freq);
            Config.DataGranularity = window;

            
            
            Func<IStreamable<Empty, Signal>> data = () =>
            {
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                        .Cache()
                    ;
            };

            Func<IStreamable<Empty, Signal>> abp_data = () =>
            {
                const int freq = 125;
                const int period = 1000 / freq;
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                    ;
            };

            Func<IStreamable<Empty, Signal>> ecg_data = () =>
            {
                const int freq = 500;
                const int period = 1000 / freq;
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                    ;
            };
            
            
            switch (testcase + "_" + engine)
            {
                case "normalize_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .Normalize(window)
                    );
                    break;
                case "normalize_lifestream":
                    time = FuseTest(data, stream =>
                            stream
                                .Normalize(period, window))
                        ;
                    break;
                case "passfilter_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .BandPassFilter(period, window, 2, 200)
                    );
                    break;
                case "passfilter_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .BandPassFilter(period, window, 2, 200)
                    );
                    break;
                case "fillconst_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .FillConst(period, gap_tol, 0)
                    );
                    break;
                case "fillconst_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .FillConst(period, gap_tol, 0)
                    );
                    break;
                case "fillmean_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .FillMean(window, period, gap_tol)
                    );
                    break;
                case "fillmean_lifestream":
                    time = FuseTest(data, stream =>
                        stream
                            .FillMean(window, period, gap_tol)
                    );
                    break;
                case "resample_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .Resample(period, period / 2)
                    );
                    break;
                case "resample_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .Resample(period, period / 2, period)
                    );
                    break;
                case "endtoend_trill":
                    count = duration * (500 + 125);
                    time = MultiNonFuseTest(abp_data, ecg_data);
                    break;
                case "endtoend_lifestream":
                    count = duration * (500 + 125);
                    time = MultiFuseTest(abp_data, ecg_data);
                    break;
                default:
                    Console.Error.WriteLine("Unknown benchmark combination {0} on {1}", testcase, engine);
                    return;
            }
            Config.StreamScheduler.Stop();
        }
        static void Main(string[] args)
        {
            
            ChangingTypes();
        }
    }
}
