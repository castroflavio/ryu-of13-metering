from operator import attrgetter
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.ofproto import ofproto_v1_3
from ryu.app import simple_switch_13 as sp_1
import numpy as np
import time,threading,sys,signal
from ryu.lib import hub
from ryu.lib.packet import packet,ethernet,ipv4,arp
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls

ETHERNET = ethernet.ethernet.__name__
IPV4 = ipv4.ipv4.__name__
ARP = arp.arp.__name__
filename="subs"
packet_burst_size=10
DEBUG=0

class SimpleMonitor(sp_1.SimpleSwitch13):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    def __init__(self, *args, **kwargs):
        super(SimpleMonitor, self).__init__(*args, **kwargs)
        aThread=hub.spawn(self._monitor)
        self.net={}
        self.datapaths={}
        self.map_s2b = np.array([[0,-1]])
        self.band = np.array([(0,0,0,0,0)])
        self.sub = np.loadtxt(filename,delimiter=',',dtype='str')
        balances=self.sub[:,2].astype(np.int32)
        self.sub=self.sub[balances>0]
        print self.sub

    def add_flowM(self, datapath, priority, match, actions,meter_id):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,actions),
                        parser.OFPInstructionMeter(meter_id,ofproto.OFPIT_METER)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        millis1 = int(round(time.time() * 1000))
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)
        header_list = dict((p.protocol_name, p) for p in pkt.protocols if type(p) != str)
        eth = header_list[ETHERNET]
        dst = eth.dst
        src = eth.src
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})
        #self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)
        #learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port
        out_port = self.mac_to_port[dpid][dst] if dst in self.mac_to_port[dpid] else ofproto.OFPP_FLOOD
        actions = [parser.OFPActionOutput(out_port)]
        #if its a border switch then install special flows metering data and droping packets
        meter_id=0
        _arp= ARP in header_list
        if _arp:
            ip=header_list[ARP].src_ip
            self.net[ip]= (in_port,dpid) if ip not in self.net else self.net[ip]
            #add meter if in the border, else 0
            meter_id=self.add_meter(datapath, header_list)
        _border=(in_port,dpid) in self.net.values()
        match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
        if _border:
            if _arp:
                if meter_id:
                    if out_port != ofproto.OFPP_FLOOD:
                        self.add_flowM(datapath,10,match,actions,meter_id)
                        print "A Flow was added to the meter id %i" %meter_id
                    send=True
                else:
                    match = parser.OFPMatch(in_port=in_port)
                    self.add_flow(datapath,20,match,[])
                    print "A flow subscription was not found in a meter id and it was dropped"
                    self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)
                    send=False
            else:
                for key,tuple in self.net.iteritems():
                    if tuple == (in_port,dpid): ip=key
                meter_id= self.MeterFromIP(ip)
                if out_port != ofproto.OFPP_FLOOD:
                    self.add_flowM(datapath,10,match,actions,meter_id)
                    print "A Flow was added to the meter id %i" %meter_id
                send=True
        else:
            if out_port != ofproto.OFPP_FLOOD:
                self.add_flow(datapath,1,match,actions)
            send=True
        if send:
            data=None
            if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
            out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
            datapath.send_msg(out)
        delta = int(round(time.time() * 1000)) - millis1
        if (delta>5):
            print "It took %i ms to process a packet." %(delta)
            self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

    def add_meter(self,datapath,header_list):
        millis1 = int(round(time.time() * 1000))
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        src_ip=header_list[ARP].src_ip
        if DEBUG: print src_ip
        meter_changed=False
        #are there subscriptions to this guy?
        if not (self.sub[:,1]==src_ip).any(): return 0
        # get subscriptions WHERE ip
        subs=self.sub[self.sub[:,1]==src_ip]
        if DEBUG: print subs
        #get band id where sub id
        a=np.in1d(self.map_s2b[:,0],subs[:,0])
        band_id=self.map_s2b[a,1]
        #if DEBUG: print self.map_s2b
        #if DEBUG: print self.band.astype(np.int32)
        #get meter for each band where dpid
        b=self.band[np.in1d(self.band[:,0],band_id)]
        if DEBUG: print b.astype(np.int32)
        #filter meters per dpid
        meters=b[b[:,4]==datapath.id,3]
        if len(meters):
            meter_id=meters[0]
            if not(datapath.id==self.net[src_ip][0]): return meter_id
        else: meter_id=self.getFreeId(self.band[:,3])

        if DEBUG: print "Meter id for incoming flow is %i"%meter_id
        for entry in subs:
            #get band where subscription
            band_table = self.map_s2b[self.map_s2b[:,0]==int(entry[0]),1]
            # if there is a new subscription then delete that meter and and create a new one
            if (not (len(band_table))):
                self.deleteMeter(meter_id,self.datapaths[datapath.id])
                bands=self.createBands(datapath,subs,meter_id)
                meter_changed=True
                break
        if not meter_changed: return meter_id
        cmd=ofproto.OFPMC_ADD
        flags=ofproto.OFPMF_KBPS
        meter_mod=parser.OFPMeterMod(datapath=datapath,command=cmd,flags=flags, meter_id=meter_id, bands=ban$
        #meter_mod=parser.OFPMeterMod(datapath,command=0, flags=1, meter_id=1,bands=bands)
        datapath.send_msg(meter_mod)
        millis2 = int(round(time.time() * 1000))
        print "It took %i ms to create meter %i in switch %i"% ((millis2-millis1),meter_id,datapath.id)
        return meter_id

    def deleteMeter(self, meter_id,datapath):
        #get bands id where meter_id
        bands_id=self.band[self.band[:,3]==meter_id,0]
        #delete s2b relation where band id has that meter id
        self.map_s2b=np.delete(self.map_s2b,np.where(np.in1d(self.map_s2b[:,1],bands_id)),0)
        self.band=np.delete(self.band,np.where(self.band[:,3]==meter_id),0)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        cmd=ofproto.OFPMC_DELETE
        meter_mod=parser.OFPMeterMod(datapath=datapath,command=cmd, meter_id=meter_id)
        datapath.send_msg(meter_mod)
        print " The meter %i was deleted with success " %meter_id

    def createBands(self, datapath, subs,meter_id):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        #get rates
        rates=(subs[subs[:,3].argsort(),3]).astype(np.int32)
        #define intermiate vector to make 3 intermidiate rate measures
        a=np.array([1,2,3])/3.0
        v0=np.append([0],rates[:-1])
        diff=(rates-v0).reshape(-1,1)
        newrates=v0.reshape(-1,1)
        newrates=(newrates + diff*a).reshape(-1)
        band=[]
        for rate in newrates[:-1]:
            #get free id
            id=self.getFreeId(self.band[:,0])
            #get subscription id from rate
            sub_id=self.getSubId(rate,subs)
            #create s2b relation
            self.map_s2b=np.append(self.map_s2b,[[sub_id,id]],0)
            #Create band
            self.band=np.append(self.band,[[id,-1,rate,meter_id,datapath.id]],0)
            band.append(parser.OFPMeterBandExperimenter(rate,packet_burst_size,id))
        id=self.getFreeId(self.band[:,0])
        sub_id=self.getSubId(newrates[-1],subs)
        self.map_s2b=np.append(self.map_s2b,[[sub_id,id]],0)
        lastband=[[id,-1,newrates[-1],meter_id,datapath.id]]
        self.band=np.append(self.band,[[id,-1,newrates[-1],meter_id,datapath.id]],0)
        band.append(parser.OFPMeterBandDrop(newrates[-1],packet_burst_size))
        return band

    def getSubId(self,rate,subs):
        subscriptions=np.array(subs)
        subscriptions[:,3]=np.append([0],subs[:-1,3])
        H=[]
        for entry in subscriptions:
            #if DEBUG: print entry
            if int(entry[3])<int(rate): H.append(entry[0])
        if len(H): return int(H[-1])
        return -1

    def getFreeId(self,id_TABLE):
        possible_id=[k for k in range(1,256) if k not in id_TABLE]
        #return first possible id, or return -1 meaning that there is no possible id
        return int(possible_id[0]) if len(possible_id) else -1

    def MeterFromIP(self,ip):
        sub=self.sub
        meter={}
        band_id=self.map_s2b[np.in1d(self.map_s2b[:,0],sub[sub[:,1]==ip,0]),1]
        meters=self.band[np.in1d(self.band[:,0],band_id),3]
        return meters[0] if len(meters) else 0

    def IPfromMeter(self,meter_id):
        bands_id=self.band[self.band[:,3]==meter_id,0]
        #delete s2b relation where band id has that meter id
        sub_id=self.map_s2b[np.in1d(self.map_s2b[:,1],bands_id),0]
        return self.sub[self.sub[:,0].astype(np.int32)==sub_id[0],1] if len(sub_id) else 0

    def _monitor(self):

        while 1:
            print "  ID      IP       BAL     RATE"
            sub = np.loadtxt(filename,delimiter=',',dtype='str')

            meters={} #Dict with ip as key and meter_id as value
            for ip in self.net:
                band_id=self.map_s2b[np.in1d(self.map_s2b[:,0],sub[sub[:,1]==ip,0]),1]
                meters_id=self.band[np.in1d(self.band[:,0],band_id),3]
                if len(meters_id):
                    meters[ip]=meters_id[0]
            print meters
            balances=sub[:,2].astype(np.int32)
            self.sub=sub[balances>0]
            subs=sub[np.where( [ip in self.net for ip in sub[:,1]] )]
            print subs
            print "    ID   LM RATE METER DPID"
            print self.band.astype(np.int32)
            meter_changed=False

            for entry in subs:#For entry in subscriptions *where ip in the network*
                id,ip,bal,rate=entry
                balance = int(bal)>0
                meter = ip in meters
                if balance: #reactivate or activate new services
                    meter_id=meters[ip] if meter else self.getFreeId(self.band[:3])
                    dpid=self.net[ip][1]
                    in_port=self.net[ip][0]
                    #get band where subscription
                    band_table = self.map_s2b[self.map_s2b[:,0]==int(id),1]
                    # if there is a new subscription then delete that meter and and create a new one
                    print band_table
                    if (not (len(band_table))):
                        datapath=self.datapaths[dpid]
                        parser=datapath.ofproto_parser
                        ofproto=datapath.ofproto
                        self.deleteMeter(meter_id,datapath)
                        SUB_IP=subs[np.where(subs[:,1]==ip)]
                        bands=self.createBands(datapath,SUB_IP,meter_id)
                        meter_changed=True
                        #reactivate flow if needed
                        #print "Do you remember that flow?"
                        #print "I guess the port was %i" %in_port
                        match = parser.OFPMatch(in_port=in_port)
                        cmd=ofproto.OFPFC_DELETE
                        del_mod=datapath.ofproto_parser.OFPFlowMod(datapath, 0, 0, 0,
                                                      ofproto.OFPFC_DELETE, 0, 0, 1,
                                                      ofproto.OFPCML_NO_BUFFER,
                                                      ofproto.OFPP_ANY, ofproto.OFPG_ANY, 0, match, [])
                        print "I am about to send the message"
                        a=datapath.send_msg(del_mod)
                        cmd=ofproto.OFPMC_ADD
                        flags=ofproto.OFPMF_KBPS
                        meter_mod=parser.OFPMeterMod(datapath=datapath,command=cmd,flags=flags, meter_id=met$
                        #meter_mod=parser.OFPMeterMod(datapath,command=0, flags=1, meter_id=1,bands=bands)
                        datapath.send_msg(meter_mod)
                        break

                     #todo recreate meter
                elif meter: # deactivate if meter
                    meter_id=meters[ip]
                    dpid=self.net[ip][1]
                    datapath=self.datapaths[dpid]
                    self.deleteMeter(meter_id,datapath)
                    in_port=self.net[ip][0]
                    match = datapath.ofproto_parser.OFPMatch(in_port=in_port)
                    self.add_flow(datapath,20,match,[])
            for ip in meters:
                dpid=self.net[ip][1]
                datapath=self.datapaths[dpid]
                parser=datapath.ofproto_parser
                req = parser.OFPMeterStatsRequest(datapath, 0,meters[ip])
                datapath.send_msg(req)
            hub.sleep(1)

    @set_ev_cls(ofp_event.EventOFPMeterStatsReply, MAIN_DISPATCHER)
    def meter_stats_reply_handler(self, ev):
        meters = []
        for stat in ev.msg.body:
            meters.append('meter_id=0x%08x len=%d flow_count=%d '
                      'packet_in_count=%d byte_in_count=%d '
                      'duration_sec=%d duration_nsec=%d '
                      'band_stats=%s' %
                      (stat.meter_id, stat.len, stat.flow_count,
                       stat.packet_in_count, stat.byte_in_count,
                       stat.duration_sec, stat.duration_nsec,
                       stat.band_stats))
            meter_id=stat.meter_id
            byte_in_count=stat.byte_in_count
            sub=self.sub
            ip=self.IPfromMeter(meter_id)

            sub_id=[]
            if ip:
             for entry in sub:
                id,sub_ip,bal,rate=entry
                if sub_ip ==ip[0]:
                    if int(bal)<=(int(byte_in_count)/1000):
                        sub_id.append(id)
             for id in sub_id:
                sub[sub[:,0]==id,2] = 0
             np.savetxt(filename,sub,delimiter=',',fmt="%s")

        #self.logger.info('MeterStats: %s', meters)

    @set_ev_cls(ofp_event.EventOFPStateChange,[MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if not datapath.id in self.datapaths:
                self.logger.debug('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.debug('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]





