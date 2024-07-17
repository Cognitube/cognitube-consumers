package com.cognitube.consumer.config;

import com.cognitube.consumer.enums.VideoStatus;
import com.cognitube.consumer.typehandler.VideoStatusTypeHandler;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-backend
 * @description Configuration file for MyBatis
 * @date 2024/5/23 20:36:24
 */
@Configuration
@MapperScan({"com.cognitube.server.app.mapper", "com.cognitube.server.admin.mapper"})
public class MyBatisConfig {

    @Bean
    public SqlSessionFactory sqlSessionFactory(DataSource dataSource) throws Exception {
        SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        sessionFactory.setDataSource(dataSource);

        org.apache.ibatis.session.Configuration configuration = new org.apache.ibatis.session.Configuration();
        configuration.setMapUnderscoreToCamelCase(true);

        TypeHandlerRegistry registry = configuration.getTypeHandlerRegistry();
        registry.register(VideoStatus.class, new VideoStatusTypeHandler());

        sessionFactory.setConfiguration(configuration);

        sessionFactory.setMapperLocations(
                new PathMatchingResourcePatternResolver().getResources("classpath*:com/cognitube/server/mapper/*.xml")
        );

        return sessionFactory.getObject();
    }
}
