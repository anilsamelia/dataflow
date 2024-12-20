package com.eviden.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import com.eviden.exception.ServiceException;
import com.eviden.models.User;
import com.eviden.repository.UserRepository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private PasswordEncoder passwordEncoder;

    // Save a user
    public User saveUser(User user) {
    	user.setCreatedOn(new Date());
    	user.setLastModifiedAt(new Date());
    	user.setPassword(passwordEncoder.encode(user.getPassword()));
        return userRepository.save(user);
    }

    // Get a user by ID
    public Optional<User> getUserById(Long id) {
    	Optional<User> useropt = userRepository.findById(id);
    	if (useropt.isPresent()) {
    		return useropt;
    	}else {
    	 throw new ServiceException("No user found",404);	
    	}
    }

    // Get all users
    public List<User> getAllUsers() {
        return userRepository.findAll();
    }

    // Delete a user by ID
    public void deleteUser(Long id) {
        userRepository.deleteById(id);
    }
    
    public User updateUser(User user) {
    	Optional<User> optuser = userRepository.findById(user.getId());
    	if (optuser.isPresent()) {
    		User ouser = optuser.get();
    		user.setCreatedOn(ouser.getCreatedOn());
    		user.setLastModifiedAt(new Date());
    		user.setPassword(passwordEncoder.encode(user.getPassword()));
    		userRepository.save(user);
    	}else {
    		throw new ServiceException("User Not found",404);
    	}
    	return user;
     }
    
    public User findByEmail(String email) {
    	return userRepository.findByEmail(email);
    }
}
