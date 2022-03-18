package cn.bigdatabc.publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;


public interface TrademarkStatMapper {

    List<Map> selectTradeSum(@Param("start_time")String startTime,
                             @Param("end_time") String endTime,
                             @Param("topN") int topN);
}
