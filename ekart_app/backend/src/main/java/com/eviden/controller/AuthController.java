package com.eviden.controller;

import java.io.IOException;
import java.security.Principal;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.eviden.exception.RestExceptionHandler;

import jdk.internal.org.jline.utils.Log;
import lombok.extern.slf4j.Slf4j;

@RestController
@CrossOrigin(origins = "http://localhost:4200")
@Slf4j
public class AuthController {

	@PreAuthorize("hasRole('ROLE_ADMIN')")
	@GetMapping("/admin/dashboard")
	public String adminDashboard() {
		return "Welcome to the Admin Dashboard!";
	}

	@PreAuthorize("hasRole('ROLE_CUSTOMER')")
	@GetMapping("/customer/dashboard")
	public String customerDashboard() {
		return "Welcome to the Customer Dashboard!";
	}

	@GetMapping("/profile")
	public Principal profile(Principal user) {
		return user;
	}
	
	@GetMapping("/success")
	public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
			Authentication authentication) throws IOException {
		
		if (!authentication.isAuthenticated()) {
			throw new IOException("authtication fail");
		}
		log.info("Loging success");
		response.sendRedirect("http://localhost:4200/products"); // Change to your Angular app URL
	}



}
