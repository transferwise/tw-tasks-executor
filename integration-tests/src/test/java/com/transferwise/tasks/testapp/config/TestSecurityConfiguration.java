package com.transferwise.tasks.testapp.config;

import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.crypto.password.PasswordEncoder;

@EnableWebSecurity
public class TestSecurityConfiguration extends WebSecurityConfigurerAdapter {

  @Autowired
  public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {
    auth.inMemoryAuthentication()
        .withUser("goodEngineer").password("q1w2e3r4")
        .authorities("ROLE_TW_TASK_MANAGEMENT")
        .and()
        .withUser("badEngineer").password("q1w2e3r4")
        .authorities("ROLE_CRACKER")
        .and()
        .withUser("piiOfficer").password("q1w2e3r4")
        .authorities("NONEXISTING_ROLE_FOR_TESTING_PURPOSES_ONLY");
  }

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.sessionManagement().disable()
        .csrf().disable()
        .authorizeRequests().anyRequest().authenticated()
        .and().httpBasic();
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
