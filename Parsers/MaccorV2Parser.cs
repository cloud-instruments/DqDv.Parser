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
using System.Reflection;
using System.Text.RegularExpressions;
using Dqdv.Types;
using log4net;

namespace Dqdv.Parser.Parsers
{
    class MaccorV2Parser : ParserBase
    {
        #region Private constants
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static readonly Regex SheetName = new Regex("^Data [0-9]+.*", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        private int _currentChannelIndex = 0;
        private bool mWhEnergyMultiplier = false;
        private bool _headerFound;

        #endregion

        #region Private types

        private class DataRow
        {
            public int CycleIndex { get; set; }
            public int StepIndex { get; set; }
            public DateTime DateTime { get; set; }
            public double StepTime { get; set; }
            public string StateCode { get; set; }
            public double Current { get; set; }
            public double Voltage { get; set; }
            public double? Capacity { get; set; }
            public double? Energy { get; set; }
            public double? Temperature { get; set; }
        }

        #endregion

        #region Private fields

        private bool _onKnownSheet;
        private int _stepIdColumnIndex;
        private int _stepTimeColumnIndex;
        private int _voltageColumnIndex;
        private int _currentColumnIndex;
        private int _capacityColumnIndex;

        private int _cycleIndexColumnIndex;
        private int _dateTimeColumnIndex;
        private int _energyColumnIndex;
        private int _temperatureColumnIndex;
        private int _stateColumnIndex;
        private int _cycleIndex = 1;
        private DateTime? _startDateTime;
        private DataRow _prev;
        private bool _isChargeHappened;
        private bool _isDischargeHappened;

        #endregion

        #region ParserBase overridden methods

        public override bool OnSheetStart(int index, string name)
        {
            Log.Info($"MaccorV2Parser.OnSheetStart: Try parse sheet #{index + 1} with name {name}");

            _onKnownSheet = name.ToLower().Equals("data", StringComparison.InvariantCultureIgnoreCase) || SheetName.IsMatch(name);
            _headerFound = false;
            return false;
        }

        public override bool OnDataRecord(int index, IDataRecord record)
        {
            if (!_onKnownSheet)
                return false;

            if (!_headerFound)
            {
                if (index > 1)
                {
                    _onKnownSheet = false;
                    return false;
                }

                if (TryParseHeader(record))
                {
                    _headerFound = true;
                    return true;
                }

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
            Log.Info($"MaccorV2Parser.TryParseHeader: Try parse header with values: {string.Join(",", record.GetRowObjects())}");

            _cycleIndexColumnIndex = record.IndexOf("Cyc#");
            if (_cycleIndexColumnIndex < 0)
                return false;

            _stepIdColumnIndex = record.IndexOf("Step");
            if (_stepIdColumnIndex < 0)
                return false;

            _stepTimeColumnIndex = record.IndexOf("StepTime");
            if (_stepTimeColumnIndex < 0)
                return false;

            _voltageColumnIndex = record.IndexOf("Volts");
            if (_voltageColumnIndex < 0)
                return false;

            _currentColumnIndex = record.IndexOf("Amps");
            if (_currentColumnIndex < 0)
                return false;

            _capacityColumnIndex = record.IndexOf("Amp-hr");
            if (_capacityColumnIndex < 0)
                return false;

            _energyColumnIndex = record.IndexOf("Watt-hr");
            if (_energyColumnIndex < 0)
                return false;

            _stateColumnIndex = record.IndexOf("State");
            if (_stateColumnIndex < 0)
                return false;

            _dateTimeColumnIndex = record.IndexOf("DPt Time");
            if (_dateTimeColumnIndex < 0)
                return false;
            
            //Can be optional
            _temperatureColumnIndex = record.IndexOf("Temp 1");

            return true;
        }

        private DataRow TryReadDataRow(IDataRecord record)
        {
            int? cycleId = record.TryGetInt32(_cycleIndexColumnIndex);
            if (cycleId == null)
                return null;

            int? stepId = record.TryGetInt32(_stepIdColumnIndex);
            if (stepId == null)
                return null;

            double? capacity = record.TryGetDouble(_capacityColumnIndex);
            if (capacity == null)
                return null;

            double? energy = record.TryGetDouble(_energyColumnIndex);
            if (energy == null)
                return null;

            double? current = record.TryGetDouble(_currentColumnIndex);
            if (current == null)
                return null;

            double? voltage = record.TryGetDouble(_voltageColumnIndex);
            if (voltage == null)
                return null;

            string state = record.TryGetString(_stateColumnIndex);
            if (state == null)
                return null;

            DateTime? dateTime = record.TryGetDateTime(_dateTimeColumnIndex);
            if (dateTime == null)
                return null;

            DataRow row = new DataRow
            {
                CycleIndex = cycleId.Value,
                StepIndex = stepId.Value,
                StateCode = state,
                Capacity = capacity.Value,
                Current = current.Value,
                Voltage = voltage.Value,
                DateTime = dateTime.Value,
                Energy = energy.Value,
            };

            double? temperature = record.TryGetDouble(_temperatureColumnIndex);
            if (temperature.HasValue)
                row.Temperature = temperature.Value;

            return row;
        }

        private void Process(DataRow row)
        {
            if (_prev != null && _prev.StepIndex != row.StepIndex && _isChargeHappened && _isDischargeHappened)
            {
                _cycleIndex++;
                _isChargeHappened = false;
                _isDischargeHappened = false;
            }

            if (_startDateTime == null)
                _startDateTime = row.DateTime;

            CycleStep cycleStep = StepNameToCycleStep(row.StateCode);

            if (_prev != null && _prev.StepIndex != row.StepIndex)
            {
                if (cycleStep == CycleStep.ChargeCC || cycleStep == CycleStep.ChargeCV)
                {
                    _isChargeHappened = true;
                }
                else if (cycleStep == CycleStep.Discharge)
                {
                    _isDischargeHappened = true;
                }
            }

            DataPoint dataPoint = new DataPoint
            {
                CycleIndex = _cycleIndex,
                CycleStep = cycleStep,
                Time = (row.DateTime - _startDateTime.Value).TotalSeconds,
                Current = 1000 * row.Current,
                Voltage = row.Voltage,
                Capacity = 1000 * row.Capacity,
                Power = (1000 * row.Current) * row.Voltage,
                Temperature = row.Temperature
            };

            if (cycleStep == CycleStep.ChargeCC || cycleStep == CycleStep.ChargeCV)
            {
                if (mWhEnergyMultiplier)
                {
                    //milliwatt-hour (mWh) to Watt-hour (Wh), mWh = Wh * 1000
                    dataPoint.Energy = row.Energy / 1000;
                }
                else dataPoint.Energy = row.Energy;
            }
            else if (cycleStep == CycleStep.Discharge)
            {
                if (mWhEnergyMultiplier)
                {
                    //milliwatt-hour (mWh) to Watt-hour (Wh), mWh = Wh * 1000
                    dataPoint.DischargeEnergy = row.Energy / 1000;
                }
                else dataPoint.DischargeEnergy = row.Energy;
            }
            
            Push(_currentChannelIndex, dataPoint);
            _prev = row;
        }

        private static CycleStep StepNameToCycleStep(string name)
        {
            switch (name.Trim().ToLower())
            {
                case "r":
                {
                    return CycleStep.Rest;
                }
                case "d":
                {
                    return CycleStep.Discharge;
                }
                case "c":
                {
                    return CycleStep.ChargeCC;
                }
                default: 
                    //throw new FormatException($"Unknown Step Name {name}");
                    return CycleStep.Rest;
            }
        }

        #endregion
    }
}
