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
using System.Data;
using System.Globalization;
using System.Reflection;
using Dqdv.Types;
using log4net;

namespace Dqdv.Parser.Parsers
{
    class LandParser : ParserBase
    {
        #region Constants
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private const double CurrentThreshold = 1e-7;
        private const double CapacityThreshold = 1e-7;
        private static readonly DateTime ExcelBaseDate = new DateTime(1899, 12, 31);
        private int _currentChannelIndex = 0;

        #endregion

        #region Private types

        private enum State
        {
            Rejected,
            Initial,
            InStepHeader,
            InStep
        }

        private class Header
        {
            public int IndexColumnIndex { get; set; }
            public int TestTimeColumnIndex { get; set; }
            public bool IsTimeInSeconds { get; set; }
            public int VoltageColumnIndex { get; set; }
            public int CurrentColumnIndex { get; set; }
            public int CapacityColumnIndex { get; set; }
            public int? StateColumnIndex { get; set; }
        }

        private class DataRow
        {
            public int Index { get; set; }
            public DateTime TestTime { get; set; }
            public double Voltage { get; set; }
            public double Current { get; set; }
            public double Capacity { get; set; }
            public double Temperature { get; set; }
            public string State { get; set; }
        }

        #endregion

        #region Private fields

        private State _state = State.Rejected;
        private Header _header;

        private int _cycleIndex = 1;
        private DateTime? _startDateTime;
        private CycleStep _currStep = CycleStep.Rest;
        private readonly HashSet<CycleStep> _steps = new HashSet<CycleStep>();

        #endregion

        #region ParserBase overridden methods

        public override bool OnSheetStart(int index, string name)
        {
            Log.Info($"LandParser.OnSheetStart: Try parse sheet #{index + 1} with name {name}");

            _state = State.Initial;
            return false;
        }

        public override bool OnDataRecord(int index, IDataRecord record)
        {
            switch (_state)
            {
                case State.Rejected:
                    return false;

                case State.Initial:
                    if (_header != null)
                    {
                        var row = TryReadDataRow(_header, record);
                        if (row != null)
                        {
                            _state = State.InStep;
                            Process(row);
                            return true;
                        }

                        _header = null;
                    }

                    if (TryParseStepHeader(record))
                    {
                        _state = State.InStepHeader;
                        _header = null;
                        return true;
                    }

                    _header = TryParseHeader(record);
                    if (_header == null)
                    {
                        _state = State.Rejected;
                        return false;
                    }

                    _state = State.InStep;
                    return true;

                case State.InStepHeader:
                    _header = TryParseHeader(record);
                    if (_header == null)
                        return true;

                    _state = State.InStep;
                    return true;

                case State.InStep:
                    if (TryParseStepHeader(record))
                    {
                        _header = null;
                        _state = State.InStepHeader;
                        return true;
                    }

                    var point = TryReadDataRow(_header, record);
                    if (point == null)
                        throw new FormatException($"Failed to parse row #{index}");

                    Process(point);
                    return true;

                default:
                    throw new ApplicationException($"Unknown state {_state}");
            }
        }

        #endregion

        #region Private methods

        private static bool TryParseStepHeader(IDataRecord record)
        {
            return record.IndexOf("Index") == 0 && record.IndexOf("Mode") == 1 && record.IndexOf("Period") == 2;
        }

        private static Header TryParseHeader(IDataRecord record)
        {
            Log.Info($"LandParser.TryParseHeader: Try parse header with values: {string.Join(",", record.GetRowObjects())}");

            var indexColumnIndex = record.IndexOfAny(new[] {"Index", "记录序号"});
            if (indexColumnIndex < 0)
                return null;

            var isTimeInSeconds = false;
            var testTimeColumnIndex = record.IndexOfAny(new[] {"TestTime", "测试时间"});
            if (testTimeColumnIndex < 0)
            {
                testTimeColumnIndex = record.IndexOfAny(new[] {"TestTime/Sec."});
                if (testTimeColumnIndex < 0)
                    return null;
                isTimeInSeconds = true;
            }

            var voltageColumnIndex = record.IndexOfAny(new[] {"Voltage/V", "电压/V"});
            if (voltageColumnIndex < 0)
                return null;

            var currentColumnIndex = record.IndexOfAny(new[] {"Current/mA", "电流/mA"});
            if (currentColumnIndex < 0)
                return null;

            var capacityColumnIndex = record.IndexOfAny(new[] {"Capacity/mAh", "容量/mAh"});
            if (capacityColumnIndex < 0)
                return null;

            var stateColumnIndex = record.IndexOfAny(new[] {"State", "状态"});

            return new Header
            {
                IndexColumnIndex = indexColumnIndex,
                TestTimeColumnIndex = testTimeColumnIndex,
                IsTimeInSeconds = isTimeInSeconds,
                VoltageColumnIndex = voltageColumnIndex,
                CurrentColumnIndex = currentColumnIndex,
                CapacityColumnIndex = capacityColumnIndex,
                StateColumnIndex = stateColumnIndex < 0 ? (int?)null : stateColumnIndex
            };
        }

        private static DataRow TryReadDataRow(Header header, IDataRecord record)
        {
            var index = record.TryGetInt32(header.IndexColumnIndex);
            if (index == null)
                return null;

            var testTime = GetTestTime(record, header.TestTimeColumnIndex, header.IsTimeInSeconds);
            if (testTime == null)
                return null;

            var voltage = record.TryGetDouble(header.VoltageColumnIndex);
            if (voltage == null)
                return null;

            var current = record.TryGetDouble(header.CurrentColumnIndex);
            if (current == null)
                return null;

            var capacity = record.TryGetDouble(header.CapacityColumnIndex);
            if (capacity == null)
                return null;

            var state = (string)null;
            if (header.StateColumnIndex != null)
            {
                state = record.TryGetString(header.StateColumnIndex.Value);
                if (state == null)
                    return null;
            }

            return new DataRow
            {
                Index = index.Value,
                TestTime = testTime.Value,
                Voltage = voltage.Value,
                Current = current.Value,
                Capacity = capacity.Value,
                State = state
            };
        }

        private void Process(DataRow row)
        {
            var step = row.State != null ? StateToCycleStep(row.State) : CurrentToCycleStep(row.Current);

            if (step != _currStep)
            {
                if (step != CycleStep.Rest)
                {
                    if (_steps.Contains(step) && Math.Abs(row.Capacity) <= CapacityThreshold)
                    {
                        _cycleIndex += 1;
                        _steps.Clear();
                    }

                    _steps.Add(step);
                }

                _currStep = step;
            }

            if (_startDateTime == null)
                _startDateTime = row.TestTime;

            var dataPoint = new DataPoint
            {
                CycleStep = step,
                CycleIndex = _cycleIndex,
                Time = (row.TestTime - _startDateTime.Value).TotalSeconds,
                Current = row.Current,
                Voltage = row.Voltage,
                Capacity = row.Capacity,
                Energy = row.Capacity * row.Voltage,
                Power = row.Current * row.Voltage,
                Temperature = row.Temperature
            };

            Push(_currentChannelIndex, dataPoint);
        }

        private static DateTime? GetTestTime(IDataRecord record, int index, bool isTimeInSeconds)
        {
            if (isTimeInSeconds)
            {
                var value = record.TryGetInt32(index);
                if (value == null)
                    return null;

                return ExcelBaseDate.AddSeconds(value.Value);
            }

            if (index < 0 || index >= record.FieldCount)
                return null;

            var fieldType = record.GetFieldType(index);

            if (fieldType == typeof(DateTime))
                return record.GetDateTime(index);

            if (fieldType == typeof(TimeSpan))
            {
                var span = record.TryGetTimeSpan(index);
                if (span != null)
                {
                    return ExcelBaseDate.Add(span.Value);
                }
            }

            if (fieldType == typeof(string))
            {
                var value = record.GetString(index).Trim();
                var hyphenIndex = value.IndexOf('-');
                if (hyphenIndex == 0 || hyphenIndex == value.Length - 1)
                    return null;

                if (!int.TryParse(value.Substring(0, hyphenIndex), NumberStyles.Integer, CultureInfo.InvariantCulture, out var days))
                    return null;

                if (!TimeSpan.TryParse(value.Substring(hyphenIndex + 1), CultureInfo.InvariantCulture, out var span))
                    return null;

                return ExcelBaseDate.AddDays(days).Add(span);
            }

            return null;
        }

        private static CycleStep StateToCycleStep(string state)
        {
            switch (state.Trim().ToLower())
            {
                case "r":
                    return CycleStep.Rest;

                case "d_cc":
                case "d_rate":
                    return CycleStep.Discharge;

                case "c_cc":
                case "c_rate":
                    return CycleStep.ChargeCC;

                case "c_cv":
                    return CycleStep.ChargeCV;

                default:
                    throw new FormatException($"Unknown state {state}");
            }
        }

        private static CycleStep CurrentToCycleStep(double current)
        {
            if (Math.Abs(current) < CurrentThreshold)
                return CycleStep.Rest;

            return current > 0 ? CycleStep.ChargeCC : CycleStep.Discharge;
        }

        #endregion
    }
}
