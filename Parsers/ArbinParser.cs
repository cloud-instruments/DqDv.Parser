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
using System.Linq;
using System.Text.RegularExpressions;
using Dqdv.Types;
using log4net;
using System.Reflection;

namespace Dqdv.Parser.Parsers
{
    /// <summary>
    /// Arbin xml parser 
    /// </summary>
    class ArbinParser : ParserBase
    {
        #region Private constans

        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static readonly Regex SheetName = new Regex("^channel_[0-9]+.*", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        private static readonly Regex SheetNameStandfordFormat = new Regex("^channel_[aA-zZ]+.*", RegexOptions.IgnoreCase | RegexOptions.Singleline);

        private const double CurrentChargeThreshhold = 1e-20;
        private const double CurrentDischargeThreshhold = -1e-20;
        private int _currentChannelIndex;

        #endregion

        #region Private types

        private class DataRow
        {
            public int CycleIndex { get; set; }
            public int StepIndex { get; set; }
            public DateTime DateTime { get; set; }
            public double Current { get; set; }
            public double Voltage { get; set; }
            public double? ChargeCapacity { get; set; }
            public double? DischargeCapacity { get; set; }
            public double? Power { get; set; }
            public double? Energy { get; set; }
            public double? DischargeEnergy { get; set; }
            public double? Temperature { get; set; }
        }

        #endregion

        #region Private fields

        private bool _onKnownSheet;
        private int _cycleIndexColumnIndex;
        private int _stepIndexColumnIndex;
        private int _dateTimeColumnIndex;
        private int _currentColumnIndex;
        private int _dischargeEnergyColumnIndex;
        private int _chargeEnergyColumnIndex;
        private int _voltageColumnIndex;
        private int _chargeCapacityColumnIndex;
        private int _dischargeCapacityColumnIndex;
        private int _temperatureColumnIndex;
        
        private DataRow _buffered;
        private DataRow _prev;
        private bool lastStepDischarge = false;
        private CycleStep? _currStep;
        private int _cycleIndex;
        private DateTime? _startDateTime;

        #endregion

        #region ParserBase overridden methods

        public override bool OnSheetStart(int index, string name)
        {
            Log.Info($"ArbinParser.OnSheetStart: Try parse sheet #{index+1} with name {name}");

            _onKnownSheet = SheetName.IsMatch(name) || SheetNameStandfordFormat.IsMatch(name);

            if (_onKnownSheet)
            {
                string[] parts = name.Split('_', '-');
                // There few formats of channel numbers in Excel: "Channel_10_1" or "Channel_1-007"
                // In some cases channel index is third like in 007 and in some cases it is second
                if (parts.Length == 3 && parts[0].ToLower() == "channel")
                {
                    int result;
                    int indexOfChannel = 1;
                    if (name.Contains('-'))
                    {
                        indexOfChannel = 2;
                    }

                    if (int.TryParse(parts[indexOfChannel], out result))
                    {
                        if (_currentChannelIndex == 0 || _currentChannelIndex != result)
                        {
                            _currentChannelIndex = result;
                            //This is new channel sheet and we need to reset
                            _prev = null;
                            _startDateTime = null;
                            _currStep = null;
                            _buffered = null;
                            _cycleIndex = 0;
                        }
                    }
                }
            }

            return false;
        }

        public override bool OnDataRecord(int index, IDataRecord record)
        {
            if (!_onKnownSheet)
                return false;

            if (index == 0)
            {
                if (TryParseHeader(record))
                    return true;

                _onKnownSheet = false;
                return false;
            }

            var point = TryReadDataRow(record);
            if (point == null)
                throw new FormatException($"Failed to parse row #{index}");

            Process(point);
            return true;
        }

        public override void OnDocumentEnd()
        {
            if (_buffered != null)
            {
                Push(_buffered);
                _buffered = null;
            }
        }

        #endregion

        #region Private methods

        private bool TryParseHeader(IDataRecord record)
        {
            Log.Info($"ArbinParser.OnSheetStart: Try parse header with values: {string.Join(",", record.GetRowObjects())}");

            _cycleIndexColumnIndex = record.IndexOf("Cycle_Index");
            if (_cycleIndexColumnIndex < 0)
                return false;

            _stepIndexColumnIndex = record.IndexOf("Step_Index");
            if (_stepIndexColumnIndex < 0)
                return false;

            _dateTimeColumnIndex = record.IndexOfAny(new [] { "Date_Time", "DateTime" });
            if (_dateTimeColumnIndex < 0)
                return false;

            _currentColumnIndex = record.IndexOfAny(new [] { "Current(A)", "Current" });
            if (_currentColumnIndex < 0)
                return false;

            _voltageColumnIndex = record.IndexOfAny(new [] { "Voltage(V)", "Voltage" });
            if (_voltageColumnIndex < 0)
                return false;

            _chargeCapacityColumnIndex = record.IndexOfAny(new [] { "Charge_Capacity(Ah)", "Charge_Capacity" });
            if (_chargeCapacityColumnIndex < 0)
                return false;

            _dischargeCapacityColumnIndex = record.IndexOfAny(new[] { "Discharge_Capacity(Ah)", "Discharge_Capacity" });
            if (_dischargeCapacityColumnIndex < 0)
                return false;

            _chargeEnergyColumnIndex = record.IndexOfAny(new[] { "Charge_Energy(Wh)", "Charge_Energy" });
            if (_chargeEnergyColumnIndex < 0)
                return false;

            _dischargeEnergyColumnIndex = record.IndexOfAny(new[] { "Discharge_Energy(Wh)", "Discharge_Energy" });
            if (_dischargeEnergyColumnIndex < 0)
                return false;

            _temperatureColumnIndex = record.IndexOfAny(new[] { "Aux_Temperature_1" });

            return true;
        }

        private DataRow TryReadDataRow(IDataRecord record)
        {
            var cycleIndex = record.TryGetInt32(_cycleIndexColumnIndex);
            if (cycleIndex == null)
                return null;

            var stepIndex = record.TryGetInt32(_stepIndexColumnIndex);
            if (stepIndex == null)
                return null;

            var dateTime = record.TryGetDateTime(_dateTimeColumnIndex);
            if (dateTime == null)
            {
                var rawOleDate = record.TryGetDouble(_dateTimeColumnIndex); // date may be present in OLE format
                if (rawOleDate == null)
                    return null;
                dateTime = DateTime.FromOADate(rawOleDate.Value);
            }

            var current = record.TryGetDouble(_currentColumnIndex);
            if (current == null)
                return null;

            var voltage = record.TryGetDouble(_voltageColumnIndex);
            if (voltage == null)
                return null;

            var power = current * voltage;        
            var chargeCapacity = record.TryGetDouble(_chargeCapacityColumnIndex);
            var dischargeCapacity = record.TryGetDouble(_dischargeCapacityColumnIndex);

            var chargeEnergy = record.TryGetDouble(_chargeEnergyColumnIndex);
            var dischargeEnergy = record.TryGetDouble(_dischargeEnergyColumnIndex);
            
            var temperature = record.TryGetDouble(_temperatureColumnIndex);

            return new DataRow
            {
                CycleIndex = cycleIndex.Value,
                StepIndex = stepIndex.Value,
                DateTime = dateTime.Value,
                Current = current.Value,
                Voltage = voltage.Value,
                ChargeCapacity = chargeCapacity,
                DischargeCapacity = dischargeCapacity,
                Power = power,
                Energy = chargeEnergy,
                DischargeEnergy = dischargeEnergy,
                Temperature = temperature
            };
        }

        private void Process(DataRow row)
        {
            if (_buffered != null)
            {
                if (row.StepIndex != _buffered.StepIndex)
                {
                    _currStep = CycleStep.ChargeCV;
                    lastStepDischarge = false;
                    Push(_buffered);
                }
                else
                {
                    _currStep = GetChargeType(_buffered, row);
                    lastStepDischarge = false;
                    Push(_buffered);
                }

                _prev = _buffered;
                _buffered = null;
            }

            if (_prev == null || row.StepIndex != _prev.StepIndex)
            {
                if (row.Current > CurrentChargeThreshhold)
                {
                    _currStep = null;
                    _buffered = row;
                    lastStepDischarge = false;
                    return;
                }

                //transition state between cycles, can be with 0 discharge capacity
                if (_prev != null &&
                    lastStepDischarge &&
                    row.StepIndex != _prev.StepIndex &&
                    row.Current <= CurrentDischargeThreshhold)
                {
                    _currStep = CycleStep.Rest;
                    lastStepDischarge = false;
                }
                else
                {
                    if (row.Current <= CurrentDischargeThreshhold)
                    {
                        lastStepDischarge = true;
                        _currStep = CycleStep.Discharge;
                    }
                    else
                    {
                        _currStep = CycleStep.Rest;
                        lastStepDischarge = false;
                    }
                }
            }

            Push(row);
            _prev = row;
        }

        private static CycleStep GetChargeType(DataRow curr, DataRow next)
        {
            var dc = Math.Abs(curr.Current - next.Current) / curr.Current;
            var dv = Math.Abs(curr.Voltage - next.Voltage) / curr.Voltage;
            return dc > dv ? CycleStep.ChargeCV : CycleStep.ChargeCC;
        }

        private void Push(DataRow row)
        {
            if (_prev == null || row.CycleIndex != _prev.CycleIndex)
            {
                _cycleIndex = row.CycleIndex;
            }

            if (_startDateTime == null)
                _startDateTime = row.DateTime;

            double? capacity;
            switch (_currStep)
            {
                case CycleStep.ChargeCC:
                case CycleStep.ChargeCV:
                    capacity = row.ChargeCapacity;
                    break;
                case CycleStep.Discharge:
                    capacity = row.DischargeCapacity;
                    break;
                default:
                    capacity = null;
                    break;
            }

            if (_currStep != null)
            {
                var point = new DataPoint
                {
                    CycleIndex = _cycleIndex,
                    CycleStep = _currStep.Value,
                    Time = (row.DateTime - _startDateTime.Value).TotalSeconds,
                    Current = 1000 * row.Current,
                    Voltage = row.Voltage,
                    Capacity = 1000 * capacity,
                    Energy = row.Energy,
                    DischargeEnergy = row.DischargeEnergy,
                    Power = (1000 * row.Current) * row.Voltage,
                    Temperature = row.Temperature
                };

                Push(_currentChannelIndex, point);
            }
        }

        #endregion
    }
}
