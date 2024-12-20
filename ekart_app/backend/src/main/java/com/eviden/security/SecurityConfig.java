package com.eviden.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;
import com.eviden.handler.CustomAuthenticationSuccessHandler;
import com.eviden.service.CustomOAuth2UserService;

@Configuration
@EnableWebSecurity

public class SecurityConfig implements WebMvcConfigurer {

	
	@Autowired
    private CustomOAuth2UserService customOAuth2UserService;
	
	@Autowired
	private  CustomAuthenticationSuccessHandler successHandler;
	
	@Bean
	public PasswordEncoder getPassEncoder() {
		return new  BCryptPasswordEncoder();
	}
	
	@Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**").allowedOrigins("http://localhost:4200");
    }
	
	
    @Bean
    public SecurityFilterChain configure(HttpSecurity http) throws Exception {
        http
        .authorizeRequests()
            .antMatchers("/product/**","/profile/**").permitAll()
            .anyRequest().authenticated()
        .and()
            .oauth2Login()
            .userInfoEndpoint()
            .userService(customOAuth2UserService)
            // Use custom service
        .and()
        .successHandler(successHandler)
        .and()
        .logout(
        		logout -> logout
	            .logoutUrl("/logout")  // Default logout URL
	            .logoutSuccessUrl("/login?logout")  // Redirect to login page after logout
	            .invalidateHttpSession(true)  // Invalidate session
	            .clearAuthentication(true)  // Clear authentication
	            .deleteCookies("JSESSIONID") );//.logoutSuccessUrl("/").permitAll();
        
        return http.build();
        
    }
}