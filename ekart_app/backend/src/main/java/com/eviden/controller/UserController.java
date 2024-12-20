package com.eviden.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;

import org.springframework.web.bind.annotation.*;
import com.eviden.models.User;
import com.eviden.service.UserService;
import io.swagger.annotations.Api;


import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/user")
@Api("User API management")
public class UserController {

	@Autowired
	private UserService userService;



	@PostMapping
	public User createUser(@RequestBody User user) {
		return userService.saveUser(user);
	}
	


	@GetMapping("/{id}")
	public ResponseEntity<User> getUserById(@PathVariable Long id) {
		Optional<User> useroption = userService.getUserById(id);
		User user = useroption.get();
		return ResponseEntity.ok(user);
	}

	@GetMapping
	@PreAuthorize("hasRole('ADMIN')")
	public List<User> getAllUsers() {
		return userService.getAllUsers();
	}

	@DeleteMapping("/{id}")
	@PreAuthorize("hasRole('ADMIN')")
	public ResponseEntity<Void> deleteUser(@PathVariable Long id) {
		userService.deleteUser(id);
		return ResponseEntity.ok().build();
	}

	@PutMapping
	@PreAuthorize("hasRole('CUSTOMER') or hasRole('VENDOR') or hasRole('ADMIN')")
	public User updateUser(@RequestBody User user) {
		return userService.updateUser(user);
	}
	

}
