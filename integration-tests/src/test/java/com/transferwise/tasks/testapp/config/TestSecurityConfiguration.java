package com.transferwise.tasks.testapp.config;

import java.util.Objects;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.DefaultSecurityFilterChain;

@Configuration
@EnableWebSecurity
public class TestSecurityConfiguration {

  @Bean
  public DefaultSecurityFilterChain twTasksSecurityFilterChain(HttpSecurity http) throws Exception {
    http.sessionManagement().disable()
        .csrf().disable()
        .authorizeHttpRequests().anyRequest().authenticated()
        .and().httpBasic();

    return http.build();
  }

  @Bean
  public InMemoryUserDetailsManager userDetailsService() {
    var user0 = User.builder()
        .username("goodEngineer")
        .password("q1w2e3r4")
        .authorities("ROLE_DEVEL")
        .build();

    var user1 = User.builder()
        .username("badEngineer")
        .password("q1w2e3r4")
        .authorities("ROLE_CRACKER")
        .build();

    var user2 = User.builder()
        .username("piiOfficer")
        .password("q1w2e3r4")
        .authorities("NONEXISTING_ROLE_FOR_TESTING_PURPOSES_ONLY")
        .build();

    return new InMemoryUserDetailsManager(user0, user1, user2);
  }

  @Bean
  public PasswordEncoder passwordEncoder() {
    return new PasswordEncoder() {
      @Override
      public String encode(CharSequence rawPassword) {
        return rawPassword.toString();
      }

      @Override
      public boolean matches(CharSequence rawPassword, String encodedPassword) {
        return Objects.equals(rawPassword, encodedPassword);
      }
    };
  }
}
