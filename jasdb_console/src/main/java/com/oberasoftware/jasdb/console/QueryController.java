package com.oberasoftware.jasdb.console;

import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.session.DBSession;
import com.oberasoftware.jasdb.api.session.DBSessionFactory;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.EntityBag;
import com.oberasoftware.jasdb.api.session.query.QueryBuilder;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.console.model.PageResult;
import com.oberasoftware.jasdb.console.model.SearchForm;
import com.oberasoftware.jasdb.console.model.WebEntity;
import com.oberasoftware.jasdb.core.SimpleEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

/**
 * @author Renze de Vries
 */
@Controller
public class QueryController {
    private static final Logger LOG = LoggerFactory.getLogger(QueryController.class);

    @Autowired
    private DBSessionFactory sessionFactory;

    @RequestMapping(value = "/console/query", method = GET)
    public String findAll(Model model) throws JasDBException {
        LOG.info("Requesting query page");
        return "data/query";
    }

    @RequestMapping(value = "/console/query/{instanceId}/{bag}")
    public String findAll(@PathVariable String instanceId, @PathVariable String bag, Model model) throws JasDBException {
        model.addAttribute("instance", instanceId);
        model.addAttribute("bag", bag);

        return "data/query";
    }
}
