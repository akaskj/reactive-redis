package hello;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;

@Configuration
class AppConfig {

//  @Primary
//  @Bean
//  ReactiveRedisConnectionFactory reactiveRedisConnectionFactory() {
//    return new LettuceConnectionFactory("jsk-sse-poc.yqomx7.ng.0001.usw2.cache.amazonaws.com", 6379);
//  }


    @Bean
    public ReactiveRedisTemplate<String, String> reactiveRedisTemplateString
            (ReactiveRedisConnectionFactory connectionFactory) {
        return new ReactiveRedisTemplate<>(connectionFactory, RedisSerializationContext.string());
    }
}