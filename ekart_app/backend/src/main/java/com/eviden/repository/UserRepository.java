package com.eviden.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.eviden.models.User;

public interface UserRepository extends JpaRepository<User, Long> {

	User findByEmail(String username);

}