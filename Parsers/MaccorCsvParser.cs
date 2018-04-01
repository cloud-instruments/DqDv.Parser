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
using Dqdv.Types;
using log4net;

namespace Dqdv.Parser.Parsers
{
    /// <summary>
    /// Maccor csv parse
    /// </summary>
    class MaccorCsvParser : ParserBase
    {
        ////////////////////////////////////////////////////////////
        // Constants, Enums and Class members
        ////////////////////////////////////////////////////////////

        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private int _cycleIndexColumnIndex;
        private int _stepIndexColumnIndex;
        private int _currentColumnIndex;
        private int _voltageColumnIndex;
        private int _capacityColumnIndex;
        private int _energyColumnIndex;
        private int _stateColumnIndex;
        private int _dateTimeColumnIndex;
        private DateTime? _startDateTime;
        private bool _isKnown;

        private class DataRow
        {
            public int CycleIndex { get; set; }
            public int StepIndex { get; set; }
            public DateTime DateTime { get; set; }
            public double Current { get; set; }
            public double Voltage { get; set; }
            public double? Capacity { get; set; }
            public string StateCode { get; set; }
            public double? Power { get; set; }
            public double? Energy { get; set; }
            public double? Temperature { get; set; }
        }

        ////////////////////////////////////////////////////////////
        // Public Methods/Atributes
        ////////////////////////////////////////////////////////////

        public override bool OnDataRecord(int index, IDataRecord record)
        {
            if (index == 0)
            {
                return _isKnown = TryParseHeader(record);
            }

            // 1th row in maccor file can contains an additional metadata, so we need to check 2nd row in this case
            if (index == 1 && !_isKnown)
            {
                return _isKnown = TryParseHeader(record);
            }
            
            if (!_isKnown)
                return false;

            var point = TryReadDataRow(record);
            if (point == null)
                throw new FormatException($"Failed to parse row #{index}");

            Process(point);
            return true;
        }

        ////////////////////////////////////////////////////////////
        // Private Methods/Atributes
        ////////////////////////////////////////////////////////////

        private bool TryParseHeader(IDataRecord record)
        {
            Log.Info($"MaccorCsvParse.OnSheetStart: Try parse header with values: {string.Join(",", record.GetRowObjects())}");

            // Cycle number as incremented by the AdvCyc step
            _cycleIndexColumnIndex = record.IndexOf("Cyc#");
            if (_cycleIndexColumnIndex < 0)
                return false;

            // Step on the test procedure
            _stepIndexColumnIndex = record.IndexOf("Step");
            if (_stepIndexColumnIndex < 0)
                return false;

            // The current in A
            _currentColumnIndex = record.IndexOf("Amps");
            if (_currentColumnIndex < 0)
                return false;

            // The voltage in V
            _voltageColumnIndex = record.IndexOfAny(new[] {"Volts"});
            if (_voltageColumnIndex < 0)
                return false;

            // The capacity in Ah
            _capacityColumnIndex = record.IndexOfAny(new[] { "Amp-hr" });
            if (_capacityColumnIndex < 0)
                return false;

            // The energy in Wh
            _energyColumnIndex = record.IndexOfAny(new[] { "Watt-hr" });
            if (_energyColumnIndex < 0)
                return false;

            // The time when the data point was added to the data file
            _dateTimeColumnIndex = record.IndexOf("DPt Time");
            if (_dateTimeColumnIndex < 0)
                return false;

            // State or mode of the data point.
            _stateColumnIndex = record.IndexOfAny(new[] {"State"});
            if (_stateColumnIndex < 0)
                return false;

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

            var current = record.TryGetDouble(_currentColumnIndex);
            if (current == null)
                return null;

            var voltage = record.TryGetDouble(_voltageColumnIndex);
            if (voltage == null)
                return null;

            var capacity = record.TryGetDouble(_capacityColumnIndex);
            if (capacity == null)
                return null;

            var energy = record.TryGetDouble(_energyColumnIndex);
            if (energy == null)
                return null;

            var stateCode = record.TryGetString(_stateColumnIndex);
            if (string.IsNullOrEmpty(stateCode))
                return null;

            var dateTime = record.TryGetDateTime(_dateTimeColumnIndex);
            if (dateTime == null)
                return null;

            return new DataRow
            {
                CycleIndex = cycleIndex.Value + 1, // starts from 0
                StepIndex = stepIndex.Value,
                DateTime = dateTime.Value,
                Current = current.Value * 1000, // A -> mA
                Voltage = voltage.Value, // V
                Power = (current * 1000) * voltage, // W -> mW
                Capacity = capacity * 1000, // Ah -> mAh
                Energy = energy, // Wh
                StateCode = stateCode
            };
        }

        private void Process(DataRow row)
        {
            if (_startDateTime == null)
                _startDateTime = row.DateTime;
            var cycleStep = StepCodeToCycleStep(row.StateCode);
            var dataPoint = new DataPoint
            {
                CycleIndex = row.CycleIndex,
                CycleStep = cycleStep,
                Time = (row.DateTime - _startDateTime.Value).TotalSeconds,
                Current = row.Current,
                Voltage = row.Voltage,
                Capacity = row.Capacity,
                Energy = cycleStep == CycleStep.ChargeCC || cycleStep == CycleStep.ChargeCC ? row.Energy : null,
                DischargeEnergy = cycleStep == CycleStep.Discharge ? row.Energy : null,
                Power = row.Power,
                Temperature = row.Temperature
            };

            Push(0, dataPoint);
        }

        private static CycleStep StepCodeToCycleStep(string name)
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
                    return CycleStep.Rest;
            }
        }
    }
}
