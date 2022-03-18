package cn.bigdatabc.publisher.service.impl;

import cn.bigdatabc.publisher.mapper.TrademarkStatMapper;
import cn.bigdatabc.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTradeAmount(String startTime, String endTime, int topN) {
        return trademarkStatMapper.selectTradeSum(startTime,endTime,topN);
    }
}
