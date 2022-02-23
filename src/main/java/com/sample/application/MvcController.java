package com.sample.application;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class MvcController {
	Map<String, String> usermap = new HashMap<>();

	MvcController() {
		usermap.put("deba@gmail.com", "Password1");
		usermap.put("deba1@gmail.com", "Password2");
		usermap.put("deba2@gmail.com", "Password3");
	}

	@RequestMapping("/")
	public String home(Model model) {
		Login login = new Login();
		model.addAttribute("login", login);
		return "login";
	}

	@PostMapping("/login")
	public String loginUser(@ModelAttribute("login") Login login, Model model) {
		if (usermap.containsKey(login.getEmail())) {
			if (usermap.get(login.getEmail()).equals(login.getPassword())) {
				model.addAttribute("message", login.getEmail());
				return "index";
			}
		}
		model.addAttribute("message", "Invalid Username or password");
		return "login";
	}

	@GetMapping("/register")
	public String showForm(Model model) {
		User user = new User();
		model.addAttribute("user", user);

		List<String> professionList = Arrays.asList("Developer", "Designer", "Tester", "Architect");
		model.addAttribute("professionList", professionList);

		return "register_form";
	}

	@PostMapping("/register")
	public String submitForm(@ModelAttribute("user") User user) {
		usermap.put(user.getEmail(), user.getPassword());
		return "register_success";
	}
}
