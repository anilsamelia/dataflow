package com.eviden.handler;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;
import org.springframework.stereotype.Component;
@Component
public class CustomAuthenticationSuccessHandler  extends SimpleUrlAuthenticationSuccessHandler  {

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication) throws IOException, ServletException {
        // Capture redirect URL
        String redirectUrl = determineTargetUrl(request, response);

        // Log or process the redirect URL
        System.out.println("Redirect URL: " + redirectUrl);

        // Redirect user to the target URL
        getRedirectStrategy().sendRedirect(request, response, "http://localhost:4200/products");
    }
}
