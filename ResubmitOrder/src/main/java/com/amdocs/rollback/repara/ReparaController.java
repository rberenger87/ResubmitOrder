package com.amdocs.rollback.repara;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReparaController {
	
	@Autowired
	ReparaService reparaService;
	
    @PostMapping(value = "repara/service")
    public ResponseEntity<String> repara(
    		@RequestBody ServiceDTO service) throws Throwable 
    {
    	return new ResponseEntity<String>(reparaService.repara(service), HttpStatus.OK);
    }
  
}
