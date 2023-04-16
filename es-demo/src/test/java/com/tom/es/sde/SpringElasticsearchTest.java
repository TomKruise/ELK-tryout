package com.tom.es.sde;

import com.tom.es.interfaces.GoodsRepository;
import com.tom.es.pojo.Goods;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringElasticsearchTest {

    @Autowired
    private ElasticsearchTemplate esTemplate;

    @Autowired
    private GoodsRepository goodsRepository;

    @Test
    public void testCreateIndex() {
        esTemplate.createIndex(Goods.class);
    }

    @Test
    public void testMapping() {
        esTemplate.putMapping(Goods.class);
    }

    @Test
    public void addDocument() {
        List<Goods> list = new ArrayList<>();
        list.add(new Goods(1L, "iphone 8", "手机", "Apple", 8900.00, "http://localhost/"));
        list.add(new Goods(2L, "iphone X", "手机", "Apple", 9900.00, "http://localhost/"));
        list.add(new Goods(3L, "iphone XR", "手机", "Apple", 10900.00, "http://localhost/"));
        list.add(new Goods(4L, "iphone XS", "手机", "Apple", 11900.00, "http://localhost/"));
        list.add(new Goods(5L, "iphone 11", "手机", "Apple", 12900.00, "http://localhost/"));
        list.add(new Goods(6L, "iphone 12", "手机", "Apple", 13900.00, "http://localhost/"));
        list.add(new Goods(7L, "iphone 13", "手机", "Apple", 14900.00, "http://localhost/"));
        list.add(new Goods(8L, "iphone 14", "手机", "Apple", 15900.00, "http://localhost/"));

        goodsRepository.saveAll(list);
    }

    @Test
    public void testQueryById() {
        Optional<Goods> goodsOptional = goodsRepository.findById(3L);
        System.out.println(goodsOptional.orElse(null));
    }

    @Test
    public void testQueryAll() {
        Iterable<Goods> all = goodsRepository.findAll();
        all.forEach(System.out::println);
    }

    @Test
    public void testQueryByPrice() {
        List<Goods> list = goodsRepository.findByPriceBetween(8000d, 10000d);
        list.forEach(System.out::println);
    }

    @Test
    public void testNativeQuery() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[0], new String[0]));

        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "iphone"));

        queryBuilder.withPageable(
                PageRequest.of(0, 2,Sort.by(Sort.Direction.ASC, "price"))
        );

        queryBuilder.addAggregation(AggregationBuilders.terms("brandAgg").field("brand"));

        AggregatedPage<Goods> goods = esTemplate.queryForPage(queryBuilder.build(), Goods.class);


        long total = goods.getTotalElements();
        int totalPages = goods.getTotalPages();
        List<Goods> list = goods.getContent();
        System.out.println("total goods: " + total);
        System.out.println("total pages: " + totalPages);
        System.out.println(list);


        Aggregations aggregations = goods.getAggregations();
        Terms terms = aggregations.get("brandAgg");
        terms.getBuckets().forEach(b -> {
            System.out.println("brand: " + b.getKeyAsString());
            System.out.println("count: " + b.getDocCount());
        });
    }
}
