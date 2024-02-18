package com.oberasoftware.jasdb.console;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * @author Renze de Vries
 */

@Controller
public class IndexWebController {

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String indexRedirect() {
        return "redirect:/data/";
    }
}
