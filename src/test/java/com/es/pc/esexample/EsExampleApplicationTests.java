package com.es.pc.esexample;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
public class EsExampleApplicationTests {

    private static EmbeddedElastic embeddedElastic;

    static {
        System.setProperty("ELASTICSEARCH_URL", "http://localhost:9200");
    }

    @BeforeClass
    public static void setUp() throws Exception {
        if (embeddedElastic == null) {
            embeddedElastic = EmbeddedElastic.builder().withElasticVersion("6.2.4").withSetting(PopularProperties.HTTP_PORT, "9092")
                    .withSetting(PopularProperties.CLUSTER_NAME, "es-pc-example").withStartTimeout(60, TimeUnit.SECONDS).build().start();
        }
    }

    @AfterClass
    public static void tearDown() {
        if (embeddedElastic != null) {
            embeddedElastic.stop();
            embeddedElastic = null;
        }
    }

	@Test
	public void contextLoads() {
        System.out.println("Asfd");
	}

}
