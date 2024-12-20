package com.eviden;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

@Configuration
//@ConditionalOnProperty(prefix = "app.enable", name = "swagger", havingValue = "true")

public class SwaggerConfiguration {
	@Bean
	public Docket productApi() {
		System.out.println("\n\n...............Start enableing SWAGGER API 2.0 ..............\n\n");
		return new Docket(DocumentationType.SWAGGER_2)
			.select()
			.apis(RequestHandlerSelectors.basePackage("com.eviden.controller"))
			.paths(PathSelectors.any())
			.build();

	}
}
