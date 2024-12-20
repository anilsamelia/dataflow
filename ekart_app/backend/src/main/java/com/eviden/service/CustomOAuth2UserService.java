package com.eviden.service;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.oauth2.client.userinfo.DefaultOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Service;

import com.eviden.models.User;
import com.eviden.models.auth.CustomOAuth2User;
import com.eviden.repository.UserRepository;
import com.eviden.util.Role;

@Service
public class CustomOAuth2UserService extends DefaultOAuth2UserService {
	
	@Autowired
	private UserRepository userRepository;
	
	
	
	 @Override
	    public OAuth2User loadUser(OAuth2UserRequest userRequest) throws OAuth2AuthenticationException {
		     OAuth2User oAuth2User = super.loadUser(userRequest);
	        String email = oAuth2User.getAttribute("email");
	        User user = userRepository.findByEmail(email);
			if(user == null) {
			 String client = userRequest.getClientRegistration().getClientName();
				user = new User(oAuth2User.getAttribute("name"), email,"", Role.ROLE_CUSTOMER, client);
				user.setCreatedOn(new Date());
				user.setLastModifiedAt(new Date());
				user = userRepository.save(user);
				//throw new UsernameNotFoundException("No user found");
			}
		    String role = user.getRole().toString();
	        Set<SimpleGrantedAuthority> authorities;
	        authorities = Collections.singleton(new SimpleGrantedAuthority(role));
	        System.out.println("User: loaded : "+ email + " ");
	        return new CustomOAuth2User(oAuth2User, authorities);
	    }

}
