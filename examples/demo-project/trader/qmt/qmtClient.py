import grpc
import trader.qmt.qmt_pb2 as qmt_pb2
import trader.qmt.qmt_pb2_grpc as qmt_pb2_grpc
import time
import finhack.library.log as Log
from trader.qmt.dictobj import DictObj

class qmtClient():
    def __init__(self) -> None:
        self.channel = grpc.insecure_channel('192.168.8.119:50051')
        # 使用该通道创建一个存根
        self.stub = qmt_pb2_grpc.QmtServiceStub(self.channel)

    def protobuf_to_dict(self,obj):
        # Base case for recursion termination
        if not hasattr(obj, 'DESCRIPTOR'):
            return obj

        result = {}
        for field in obj.DESCRIPTOR.fields:
            value = getattr(obj, field.name)
            # Convert repeated message fields to list of dicts
            if field.message_type and field.label == field.LABEL_REPEATED:
                result[field.name] = [self.protobuf_to_dict(v) for v in value]
            # Convert non-repeated message fields to dict
            elif field.message_type:
                result[field.name] = self.protobuf_to_dict(value)
            # Convert repeated scalar fields to list
            elif field.label == field.LABEL_REPEATED:
                result[field.name] = list(value)
            # Convert scalar fields directly
            else:
                result[field.name] = value
        return result

    def assetSync(self,context):
        Log.logger.info("开始同步资产数据")
        asset= self.getAsset()
        context['portfolio']['cash']=asset['cash']
        context['portfolio']['transferable_cash']=asset['cash']-asset['frozen_cash']
        context['portfolio']['locked_cash']=asset['frozen_cash']
        context['portfolio']['total_value']=asset['total_value']
        context['portfolio']['positions_value']=asset['market_value']
        context['portfolio']['portfolio_value=0']=asset['market_value']
        Log.logger.info("结束同步资产数据")
        return asset

    def positionSync(self,context):
        Log.logger.info("开始同步持仓信息")
        positions={

        }
        pos_list= self.GetPositions()
        for pos in pos_list:
            positions[pos['stock_code']]={
                "code":pos['stock_code'],
                "amount":pos['volume'],
                "enable_amount":pos['can_use_volume'],
                "last_sale_price":pos['open_price'],
                "cost_basis":0,
                "total_value":pos['market_value'],
            }
        context['portfolio']['positions']=positions
        Log.logger.info("结束同步持仓信息")
        return positions

    def sync(self,context): 
        Log.logger.info("同步实盘数据")
        self.assetSync(context)
        self.positionSync(context)


    def getPrice(self,code):
        try:
            response=self.stub.GetPrice(qmt_pb2.PriceRequest(code=code))
            asset= self.protobuf_to_dict(response)
            return asset['last_price']
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None


    def getInfo(self,code):
        try:
            response=self.stub.GetDailyInfo(qmt_pb2.DailyInfoRequest(code=code))
            info=self.protobuf_to_dict(response)
            info['downLimit']=info['down_limit']
            info['upLimit']=info['up_limit']
            return DictObj(info)
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None
        
    def OrderBuy(self,code, amount, price=0, strategy='strategy', remark='remark'):
        try:
            Log.logger.info(f"向qmtServer下单买入{code}共{amount}股，单价{price}]")
            # response=self.stub.OrderBuy(qmt_pb2.OrderRequest(code=code, amount=amount, price=price, strategy=strategy, remark=remark))
            # seq=self.protobuf_to_dict(response)
            return seq['seq']
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None
        
    def OrderSell(self,code, amount, price=0, strategy='strategy', remark='remark'):
        try:
            Log.logger.info(f"向qmtServer下单卖出{code}共{amount}股，单价{price}")
            # response=self.stub.OrderSell(qmt_pb2.OrderRequest(code=code, amount=amount, price=price, strategy=strategy, remark=remark))
            # seq=self.protobuf_to_dict(response)
            return seq
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None
        
    def QueryOrders(self):
        try:
            Log.logger.info(f"向qmtServer查询订单")
            response=self.stub.QueryOrders(qmt_pb2.QueryOrdersRequest())
            orders=self.protobuf_to_dict(response)
            return orders
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None
        
    def CancelOrders(self):
        try:
            response=self.stub.CancelOrders(qmt_pb2.QueryOrdersRequest())
            return True
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None   
    def RetryOrders(self):
        try:
            response=self.stub.RetryOrders(qmt_pb2.QueryOrdersRequest())
            return True
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None   

    def getAsset(self):
        try:
            print(111111111)
            response=self.stub.GetAsset(qmt_pb2.AssetRequest())
            print(222222222)
            asset=self.protobuf_to_dict(response)
            return asset
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None
        
    def GetPositions(self):
        try:
            response=self.stub.GetPositions(qmt_pb2.AssetRequest())
            positions=self.protobuf_to_dict(response)
            return positions['positions']
        except grpc._channel._InactiveRpcError as e:
            Log.logger.error(f"RPC failed: {e.code()}")
            Log.logger.error(f"Details: {e.details()}")
            return None
        except Exception as e:
            Log.logger.error(f"An unexpected error occurred: {e}")
            return None
qclient=qmtClient()