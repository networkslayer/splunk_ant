# ANT - Cadence, Speed Sensor AND Heart Rate Monitor - Example
#
# Copyright (c) 2012, Gustav Tiger <gustav@tiger.name>
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import absolute_import, print_function

from ant.easy.node import Node
from ant.easy.channel import Channel
from ant.base.message import Message
from splunk_hec_handler import SplunkHecHandler

import logging
import struct
import threading
import sys
import time
import json 

NETWORK_KEY= [0xb9, 0xa5, 0x21, 0xfb, 0xbd, 0x72, 0xc3, 0x45]


logger = logging.getLogger('SplunkHec')
logger.setLevel(logging.DEBUG)

splunk_handler = SplunkHecHandler('18.130.19.186', '1213b686-f614-48b3-98a2-0e5bc26886a8', sourcetype='ant:generic', port=8088, proto='https', ssl_verify=False, source='ant:hec')
logger.addHandler(splunk_handler)

class Monitor():

    def __init__(self):
        self.heartrate = "-1";
        self.cadence = "-1";
        self.speed = "-1";
        self.power = "-1";
        self.f = open("ant_monitor.txt", "a")

    def on_data_heartrate(self, data):
        self.hr_m_time = str(data[6]*256)
        self.hr = str(data[7])
        self.type = 'heart_rate'
        self.hr_data_page = hex(data[0])
        deviceNumberLSB = data[9]
        deviceNumberMSB = data[10]
        deviceNumber = "{}".format(deviceNumberLSB + (deviceNumberMSB<<8))
        deviceType = "{}".format(data[11])
        self.rec_time=str(time.time())
        self.hr_data = {} 
        self.hr_data.update({'time': time.time()})
        self.hr_data.update({'event': {'type': self.type, 'data_type': self.hr_data_page, 'hr_m_time': self.hr_m_time, 'heart_rate':self.hr, 'device_id': deviceNumber, 'device_type':deviceType}})

        self.write_log(self.hr_data)
        return self.hr_data

    def on_data_cadence_speed(self, data):
        # 0x4e,channel,uint16_le_diff:cadence_measurement_time,uint16_le_diff:crank_revs,uint16_le_diff:speed_measurement_time,uint16_le_diff:wheel_revs
        self.cadence_m_time = str(data[2]*256)
        self.cadence_speed_m_time = str(data[4]*256)
        self.cadence = str(data[3]*256 + data[2])
        self.speed = str(data[7]*256 + data[6])
        self.type='speed_cadence'
        self.spd_data_page = hex(data[0])
        deviceNumberLSB = data[9]
        deviceNumberMSB = data[10]
        deviceNumber = "{}".format(deviceNumberLSB + (deviceNumberMSB<<8))
        deviceType = "{}".format(data[11])
        self.rec_time = time.time()
        
        self.spd_data = {}
        self.spd_data.update({"time": time.time(), "sourcetype": "ant:cadence"})
        self.spd_data.update({'event': {'type': self.type, 'data_type': self.spd_data_page, 'speed': self.speed, 'cadence': self.cadence, 'device_id': deviceNumber, 'device_type':deviceType }})
        self.write_log(self.spd_data)

    def on_data_power(self, data):
        print(data)
        self.pwr_data_page = hex(data[0])
        self.pwr_counter = str(data[1])
        self.pwr_cadence = str(data[3])
        self.pwr_accum = str(data[4])
        self.power = str(data[6])
        self.type = "power_meter"
        deviceNumberLSB = data[9]
        deviceNumberMSB = data[10]
        deviceNumber = "{}".format(deviceNumberLSB + (deviceNumberMSB<<8))
        deviceType = "{}".format(data[11])

        self.pwr_data = {}
        self.pwr_data.update({'time': time.time()})
        self.pwr_data.update({'event': {'type': self.type, 'data_type': self.pwr_data_page, 'event_counter': self.pwr_counter, 'cadence': self.pwr_cadence, 'device_id': deviceNumber, 'device_type': deviceType, 'power': self.power, 'accumulative_power': self.pwr_accum}})
        self.write_log(self.pwr_data)


    def device_info(self, data):
        deviceNumberLSB = data[9]
        deviceNumberMSB = data[10]
        deviceNumber = "{}".format(deviceNumberLSB + (deviceNumberMSB<<8))
        deviceType = "{}".format(data[11])
        return str(deviceNumber)

    def display(self):
        string = "Power: " + self.power + "Heartrate: " + self.heartrate + " Pedal revolutions: " + self.cadence + " Wheel revolutions: " + self.speed
        return
        sys.stdout.write(string)
        sys.stdout.flush()
        sys.stdout.write("\b" * len(string))

    def write_log(self, curatedData):
        print('writing to log: {} '.format(curatedData))
        try:
            logger.info(curatedData)
        except Exception as e:
            print('Logging to Splunk Failed: {}'.format(e))

        self.f.write(json.dumps(curatedData,ensure_ascii=True))
        self.f.write('\n')

def main():
    #logging.basicConfig()
  try:


    hr_monitor = Monitor()
    sp_monitor = Monitor()
    pr_monitor = Monitor()

    node = Node()
    node.set_network_key(0x00, NETWORK_KEY)

    # Heart Rate Monitor
    channel = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
    print('Heart Rate Channel: {}'.format(channel))

    channel.on_broadcast_data = hr_monitor.on_data_heartrate
    print(channel.on_broadcast_data)
    channel.on_burst_data = hr_monitor.on_data_heartrate

    channel.set_period(32280)
    channel.set_search_timeout(0xFF)
    channel.enable_extended_messages(1)
    channel.set_rf_freq(57)
    channel.set_id(0, 120, 0)

    # Cadence_Speed Sensor
    channel_cadence_speed = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
    print('SPD/Cadence Channel: {}'.format(channel))

    channel_cadence_speed.on_broadcast_data = sp_monitor.on_data_cadence_speed
    channel_cadence_speed.on_burst_data = sp_monitor.on_data_cadence_speed

    channel_cadence_speed.set_period(32472)
    channel_cadence_speed.set_search_timeout(0xFF)
    channel_cadence_speed.enable_extended_messages(1)
    channel_cadence_speed.set_rf_freq(57)
    channel_cadence_speed.set_id(0, 121, 0)

    # Power Output
    channel_power = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
    print('Power Channel: {}'.format(channel))
    channel_power.on_broadcast_data = pr_monitor.on_data_power
    channel_power.on_burst_data = pr_monitor.on_data_power

    channel_power.set_period(8182)
    channel_power.set_search_timeout(0xFF)
    channel_power.enable_extended_messages(1)
    channel_power.set_rf_freq(57)
    channel_power.set_id(0, 11, 0)

    print('Attempting try block')
    try:
        channel_power.open()
        channel_cadence_speed.open()
        channel.open()
        node.start()
    finally:
        node.stop()

  except KeyboardInterrupt:
    print('Cleaning Up')
    try:
        node.stop()
    except:
        pass
    
if __name__ == "__main__":
    main()

