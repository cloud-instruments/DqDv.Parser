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
    class NewareV2Parser : ParserBase
    {
        #region Private constans

        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly Regex SheetName = new Regex("^Detail_[0-9_]+.*", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        private int _currentChannelIndex;
        private bool mWhEnergyMultiplier = false;

        #endregion

        #region Private types

        private class DataRow
        {
            public int CycleId { get; set; }
            public int StepId { get; set; }
            public string StepName { get; set; }
            public double Current { get; set; }
            public double Voltage { get; set; }
            public double Capacity { get; set; }
            public DateTime Realtime { get; set; }
            public double Temperature { get; set; }
            public double Energy { get; set; }
        }

        #endregion

        #region Private fields

        private bool _onKnownSheet;
        private bool _headerFound;
        private int _cycleIdColumnIndex;
        private int _stepIdColumnIndex;
        private int _stepNameColumnIndex;
        private int _currentColumnIndex;
        private int _voltageColumnIndex;
        private int _capacityColumnIndex;
        private int _realtimeColumnIndex;
        private int _energyColumnIndex;

        private int _cycleIndex = 1;
        private double _totalChargeCapacity;
        private double _totalDischargeCapacity;
        private double _stepStartCapacity;
        private DateTime? _startTime;
        private DataRow _prev;

        #endregion

        #region ParserBase overridden methods

        public override bool OnSheetStart(int index, string name)
        {
            Log.Info($"NewareParser.NewareV2Parser: Try parse sheet #{index + 1} with name {name}");

            _onKnownSheet = SheetName.IsMatch(name);

            if (_onKnownSheet)
            {
                string[] parts = name.Split('_');
                if (parts.Length == 4 && parts[0] == "Detail")
                {
                    _currentChannelIndex = int.Parse(parts[3]);
                }
            }

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

            if (record.IsEmpty())
                return true;

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
            Log.Info($"NewareParser.NewareV2Parser: Try parse header with values: {string.Join(",", record.GetRowObjects())}");

            _cycleIdColumnIndex = record.IndexOf("Cycle");
            if (_cycleIdColumnIndex < 0)
                return false;

            _stepIdColumnIndex = record.IndexOf("Step");
            if (_stepIdColumnIndex < 0)
                return false;

            _stepNameColumnIndex = record.IndexOf("Status");
            if (_stepNameColumnIndex < 0)
                return false;

            _currentColumnIndex = record.IndexOf("Cur(A)");
            if (_currentColumnIndex < 0)
                return false;

            _voltageColumnIndex = record.IndexOf("Voltage(V)");
            if (_voltageColumnIndex < 0)
                return false;

            _capacityColumnIndex = record.IndexOf("Capacity(Ah)");
            if (_capacityColumnIndex < 0)
                return false;

            _realtimeColumnIndex = record.IndexOf("Absolute time");
            if (_realtimeColumnIndex < 0)
                return false;

            _energyColumnIndex = record.IndexOfAny(new[] { "Energy(Wh)", "Energy(mWh)" });
            if (_energyColumnIndex < 0)
                return false;

            return true;
        }

        private DataRow TryReadDataRow(IDataRecord record)
        {
            var cycleId = record.TryGetInt32(_cycleIdColumnIndex);
            if (cycleId == null)
                return null;

            var stepId = record.TryGetInt32(_stepIdColumnIndex);
            if (stepId == null)
                return null;

            var stepName = record.TryGetString(_stepNameColumnIndex);
            if (stepName == null)
                return null;

            var current = record.TryGetDouble(_currentColumnIndex);
            if (current == null)
                return null;

            var voltage = record.TryGetDouble(_voltageColumnIndex);
            if (voltage == null)
                return null;

            var capacity = record.TryGetDouble(_capacityColumnIndex);
            if (capacity == null)
                return null;

            var realtime = record.TryGetDateTime(_realtimeColumnIndex);
            if (realtime == null)
                return null;

            var energy = record.TryGetDouble(_energyColumnIndex);
            if (energy == null)
                return null;

            return new DataRow
            {
                CycleId = cycleId.Value,
                StepId = stepId.Value,
                StepName = stepName,
                Current = current.Value,
                Voltage = voltage.Value,
                Capacity = capacity.Value,
                Realtime = realtime.Value,
                Energy = energy.Value
            };
        }

        private void Process(DataRow row)
        {
            if (_prev != null && _prev.CycleId != row.CycleId)
            {
                _cycleIndex++;
                _totalChargeCapacity = 0;
                _totalDischargeCapacity = 0;
            }

            var cycleStep = StepNameToCycleStep(row.StepName);

            if (_prev != null && _prev.StepId != row.StepId)
            {
                if (cycleStep == CycleStep.ChargeCC || cycleStep == CycleStep.ChargeCV)
                    _stepStartCapacity = _totalChargeCapacity;
                else if (cycleStep == CycleStep.Discharge)
                    _stepStartCapacity = _totalDischargeCapacity;
                else
                    _stepStartCapacity = 0;
            }

            var capacity = _stepStartCapacity + row.Capacity;

            if (cycleStep == CycleStep.ChargeCC || cycleStep == CycleStep.ChargeCV)
                _totalChargeCapacity = capacity;
            else if (cycleStep == CycleStep.Discharge)
                _totalDischargeCapacity = capacity;

            if (_startTime == null)
                _startTime = row.Realtime;

            DataPoint dataPoint = new DataPoint();
            dataPoint.CycleIndex = _cycleIndex;
            dataPoint.CycleStep = cycleStep;
            dataPoint.Time = (row.Realtime - _startTime.Value).TotalSeconds;
            dataPoint.Current = 1000 * row.Current;
            dataPoint.Voltage = row.Voltage;
            dataPoint.Capacity = 1000 * capacity;
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
            dataPoint.Power = (1000 * row.Current) * row.Voltage;
            dataPoint.Temperature = row.Temperature;

            Push(_currentChannelIndex, dataPoint);

            _prev = row;
        }

        private static CycleStep StepNameToCycleStep(string name)
        {
            switch (name.Trim().ToLower().Trim())
            {
                case "rest":
                {
                    //Jump Condition: Time
                    return CycleStep.Rest;
                }
                case "cccv_dchg":
                case "cc_dchg":
                {
                    //Constant Current Discharge (discharging current)
                    //Jumping Condition: Time / Voltage / Capacity / Energy
                    return CycleStep.Discharge;
                }
                case "cc_chg":
                case "cccv_chg":
                {
                    //Constant Current Charge (Charging current)
                    //Jumping Condition: Time / Voltage /Capacity / Energy / Delta V
                    //CCCV_Chg - constnat current charge is followed by a constant voltage charge when the 
                    //voltage meets the settings (Charging current, charging voltage)
                    //Jumping Condition: Time / Capacity / Energy / Cut-off current
                    return CycleStep.ChargeCC;
                }

                case "cv_chg":
                {
                    //Constant Voltage Charge (Charging Voltage)
                    //Jumping Condition: Time / Capacity / Energy / Cut-off current
                    return CycleStep.ChargeCV;
                }
                default: throw new FormatException($"Unknown Step Name {name}");
            }
        }

        #endregion
    }
}
