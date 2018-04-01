/*
Copyright(c) <2018> <University of Washington>
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Dqdv.Parser.Parsers;
using Dqdv.Types;
using ExcelDataReader;
using log4net;

namespace Dqdv.Parser
{
    public class CiExcelParser
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private class ParserSink : IDataSink
        {
            public readonly Dictionary<int, List<DataPoint>> ChannelDictionary =
                new Dictionary<int, List<DataPoint>>();

            public void Push(int channel, DataPoint point)
            {
                if (!ChannelDictionary.ContainsKey(channel))
                {
                    var dataPoints = new List<DataPoint>
                    {
                        point
                    };

                    ChannelDictionary.Add(channel, dataPoints);
                }
                else
                {
                    ChannelDictionary[channel].Add(point);
                }
            }

            public DataPoint[] GetDataPoints(int channel)
            {
                return ChannelDictionary[channel].ToArray();
            }

            public void PopulateChannelDataPoints(int channel, List<DataPoint> points)
            {
                if (!ChannelDictionary.ContainsKey(channel))
                {
                    ChannelDictionary.Add(channel, points);
                }
                else
                {
                    ChannelDictionary[channel].AddRange(points);
                }
            }
        }

        public Dictionary<int, List<DataPoint>> Parse(string fileName, int projectId, string trace)
        {
            using (var stream = File.OpenRead(fileName))
            {
                return new CiExcelParser().Parse(stream, projectId, fileName, trace);
            }
        }

        public Dictionary<int, List<DataPoint>> Parse(Stream stream, int projectId, string fileName, string trace)
        {
            var sink = new ParserSink();

            var responsible = (IParser)null;
            var candidates = new List<IParser>
            {
                new ArbinParser(),
                new MaccorParser(),
                new MaccorV2Parser(),
                new MaccorCsvParser(),
                new NewareParser(),
                new NewareV2Parser(),
                new LandParser()
            };

            using (var reader = CreateReader(stream))
            {
                var sheetIndex = 0;

                do
                {
                    var sheetName = reader.Name;
                    if (responsible == null)
                    {
                        foreach (var p in candidates)
                        {
                            if (p.OnSheetStart(sheetIndex, sheetName))
                            {
                                Log.Info($"Parser for data file was found: {p.GetType().Name}");

                                responsible = p;
                                responsible.Connect(sink);
                                break;
                            }
                        }
                    }
                    else
                    {
                        responsible.OnSheetStart(sheetIndex, sheetName);
                    }

                    var rowIndex = 0;
                    while (reader.Read())
                    {
                        if (responsible == null)
                        {
                            foreach (var p in candidates)
                            {
                                if (p.OnDataRecord(rowIndex, reader))
                                {
                                    Log.Info($"CiExcelParser.Parse: Parser for data file was found: {p.GetType().Name}");

                                    responsible = p;
                                    responsible.Connect(sink);
                                    break;
                                }
                            }
                        }
                        else
                        {
                            responsible.OnDataRecord(rowIndex, reader);
                        }

                        rowIndex += 1;
                    }

                    if (responsible == null)
                    {
                        foreach (var p in candidates)
                        {
                            if (p.OnSheetEnd(sheetIndex, sheetName))
                            {
                                Log.Info($"CiExcelParser.Parse: Parser for data file was found: {p.GetType().Name}");

                                responsible = p;
                                responsible.Connect(sink);
                                break;
                            }
                        }
                    }
                    else
                    {
                        responsible.OnSheetEnd(sheetIndex, sheetName);
                    }

                    sheetIndex += 1;
                } while (reader.NextResult());

                responsible?.OnDocumentEnd();
            }

            if (responsible == null)
            {
                throw new FormatException($"CiExcelParser.Parse: Unknown file format File: {fileName}, projectId: {projectId}, trace: {trace}");
            }

            return sink.ChannelDictionary;
        }

        private IExcelDataReader CreateReader(Stream stream)
        {
            try
            {
                // try to create default excel data reader
                return ExcelReaderFactory.CreateReader(stream);
            }
            catch (Exception ex)
            {
                Log.Info($"CiExcelParser.Parse: Unable to create excel data reader. Try to create CSV reader", ex);

                // fallback action, if file not excel compatible then try to created scv reader, or raise an exception 
                return ExcelReaderFactory.CreateCsvReader(stream);
            }
        }
    }
}
