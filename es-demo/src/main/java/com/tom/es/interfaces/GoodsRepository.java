package com.tom.es.interfaces;

import com.tom.es.pojo.Goods;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface GoodsRepository extends ElasticsearchRepository<Goods,Long> {
    List<Goods> findByPriceBetween(Double from, Double to);
}
