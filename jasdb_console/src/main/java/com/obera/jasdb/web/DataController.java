package com.obera.jasdb.web;

import com.obera.jasdb.web.model.Bag;
import com.obera.jasdb.web.model.WebEntity;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.validation.Valid;

/**
 * @author renarj
 */
@Controller
@RequestMapping(value = "/data")
public class DataController {

    @Autowired
    private DBSessionFactory sessionFactory;

    @RequestMapping(value="/", method=RequestMethod.GET)
    public String indexPage(Model model) throws JasDBException {
        DBSession session = sessionFactory.createSession();
        model.addAttribute("instances", session.getInstances());
        model.addAttribute("instanceId", session.getInstanceId());
        model.addAttribute("bags", session.getBags());
        model.addAttribute("bag", new Bag());
        model.addAttribute("entity", new WebEntity());

        return "data/index";
    }

    @RequestMapping(value="/{instanceId}", method = RequestMethod.GET)
    public String instanceData(@PathVariable String instanceId, Model model) throws JasDBException {
        DBSession session = sessionFactory.createSession();

        model.addAttribute("instanceId", instanceId);
        model.addAttribute("instances", session.getInstances());
        model.addAttribute("bags", session.getBags(instanceId));
        model.addAttribute("bag", new Bag());
        model.addAttribute("entity", new WebEntity());


        return "data/index";
    }

    @RequestMapping(value="/{instanceId}/createBag", method = RequestMethod.POST)
    public String createBag(Bag bag, @PathVariable String instanceId) throws JasDBException {
        DBSession session = sessionFactory.createSession(instanceId);

        session.createOrGetBag(bag.getName());

        return "redirect:/data/";
    }

    @RequestMapping(value = "/{instanceId}/createEntity", method = RequestMethod.POST)
    public String createData(@Valid WebEntity entity, @PathVariable String instanceId) throws JasDBException {
        DBSession session = sessionFactory.createSession(instanceId);
        EntityBag entityBag = session.createOrGetBag(entity.getBag());

        entityBag.addEntity(SimpleEntity.fromJson(entity.getData()));

        return "redirect:/data/";
    }
}
