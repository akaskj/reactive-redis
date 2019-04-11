//package hello;
//
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Primary;
//import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
//import org.springframework.data.redis.connection.RedisConnectionFactory;
//import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
//
//@Configuration
//class AppConfig {
//
//  @Primary
//  @Bean
//  ReactiveRedisConnectionFactory reactiveRedisConnectionFactory() {
//    return new LettuceConnectionFactory("jsk-sse-poc.yqomx7.ng.0001.usw2.cache.amazonaws.com", 6379);
//  }
//
//}