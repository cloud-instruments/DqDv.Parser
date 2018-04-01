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
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using Dqdv.Types;
using log4net;

namespace Dqdv.Parser.Parsers
{
    class MaccorParser : ParserBase
    {
        #region Private constants
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static readonly Regex SheetName = new Regex("^Cycle ([0-9]+) (CHG|DIS)$", RegexOptions.IgnoreCase | RegexOptions.Singleline);

        #endregion

        #region Private types

        private class DataRow
        {
            public double StepTime { get; set; }
            public double Current { get; set; }
            public double Voltage { get; set; }
            public double Temperature { get; set; }
        }

        #endregion

        #region Private fields

        private bool _onKnownSheet;
        private CycleStep? _step;
        private int _originalCycleIndex;
        private int? _prevOriginalCycleIndex;

        private int _stepTimeColumnIndex;
        private int _voltageColumnIndex;
        private int _currentColumnIndex;

        private int _cycleIndex = 1;
        private bool _first = true;
        private double _globalTime;
        private double _sheetStartTime;
        private double _capacity;
        private double? _prevTime;

        #endregion

        #region ParserBase overridden methods

        public override bool OnSheetStart(int index, string name)
        {
            Log.Info($"MaccorParser.OnSheetStart: Try parse sheet #{index + 1} with name {name}");

            var match = SheetName.Match(name);
            if (match.Success && int.TryParse(match.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out _originalCycleIndex))
            {
                _step = match.Groups[2].Value == "CHG" ? CycleStep.ChargeCC : CycleStep.Discharge;
                _onKnownSheet = true;
            }
            else
            {
                _step = null;
                _onKnownSheet = false;
            }

            return false;
        }

        public override bool OnSheetEnd(int index, string name)
        {
            if (_onKnownSheet)
                _prevOriginalCycleIndex = _originalCycleIndex;

            return false;
        }

        public override bool OnDataRecord(int index, IDataRecord record)
        {
            if (!_onKnownSheet)
                return false;

            if (index == 0)
            {
                if (TryParseHeader(record))
                {
                    if (_prevOriginalCycleIndex != null && _originalCycleIndex != _prevOriginalCycleIndex)
                        _cycleIndex++;

                    _sheetStartTime = _globalTime;
                    _capacity = 0;
                    _prevTime = null;
                    return true;
                }

                _onKnownSheet = false;
                return false;
            }

            var point = TryReadDataRow(record);
            if (point == null)
                throw new FormatException($"Failed to parse row #{index}");

            Process(point);
            return true;
        }

        #endregion

        #region Private methods

        private bool TryParseHeader(IDataRecord record)
        {
            Log.Info($"MaccorParser.TryParseHeader: Try parse header with values: {string.Join(",", record.GetRowObjects())}");

            _stepTimeColumnIndex = record.IndexOf("StepTime");
            if (_stepTimeColumnIndex < 0)
                return false;

            _voltageColumnIndex = record.IndexOf("Voltage");
            if (_voltageColumnIndex < 0)
                return false;

            _currentColumnIndex = record.IndexOf("Current");
            if (_currentColumnIndex < 0)
                return false;

            return true;
        }

        private DataRow TryReadDataRow(IDataRecord record)
        {
            var stepTime = record.TryGetDouble(_stepTimeColumnIndex);
            if (stepTime == null)
                return null;

            var current = record.TryGetDouble(_currentColumnIndex);
            if (current == null)
                return null;

            var voltage = record.TryGetDouble(_voltageColumnIndex);
            if (voltage == null)
                return null;

            return new DataRow
            {
                StepTime = stepTime.Value,
                Current = current.Value,
                Voltage = voltage.Value
            };
        }

        private void Process(DataRow row)
        {
            if (_first)
            {
                _sheetStartTime = _globalTime = -row.StepTime * 60.0;
                _first = false;
            }

            double time = _sheetStartTime + row.StepTime * 60.0;
            if (time > _globalTime)
                _globalTime = time;

            if (_prevTime != null)
                _capacity += (time - _prevTime.Value) * row.Current / 3600;
            _prevTime = time;

            DataPoint dataPoint = new DataPoint
            {
                CycleIndex = _cycleIndex,
                CycleStep = _step.Value,
                Time = time,
                Current = 1000.0 * row.Current,
                Voltage = row.Voltage,
                Capacity = _capacity,
                Energy = _capacity * row.Voltage,
                Power = (1000 * row.Current) * row.Voltage,
                Temperature = row.Temperature
            };

            Push(_currentColumnIndex, dataPoint);
        }

        #endregion
    }
}
