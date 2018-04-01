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
using System.Data;
using Dqdv.Types;

namespace Dqdv.Parser
{
    abstract class ParserBase : IParser
    {
        #region Private fields

        private IDataSink _sink;

        #endregion

        #region IParser implementation

        public void Connect(IDataSink sink)
        {
            _sink = sink;
        }

        public virtual bool OnSheetStart(int index, string name)
        {
            return false;
        }

        public virtual bool OnSheetEnd(int index, string name)
        {
            return false;
        }

        public abstract bool OnDataRecord(int index, IDataRecord record);

        public virtual void OnDocumentEnd()
        {
        }

        #endregion

        #region Protected methods

        protected void Push(int channel, DataPoint point)
        {
            _sink.Push(channel, point);
        }

        protected DataPoint[] GetDataPoints(int channel)
        {
            return _sink.GetDataPoints(channel);
        }

        #endregion
    }
}
