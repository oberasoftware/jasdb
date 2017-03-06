package com.obera.jasdb.web;

import com.obera.jasdb.web.model.Bag;
import com.obera.jasdb.web.model.SearchForm;
import com.obera.jasdb.web.model.WebEntity;
import com.obera.jasdb.web.model.WebInstance;
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
        model.addAttribute("bags", session.getBags());

        return buildIndex(session, model);
    }

    @RequestMapping(value="/{instanceId}", method = RequestMethod.GET)
    public String instanceData(@PathVariable String instanceId, Model model) throws JasDBException {
        DBSession session = sessionFactory.createSession(instanceId);
        model.addAttribute("bags", session.getBags(instanceId));

        return buildIndex(session, model);
    }

    private String buildIndex(DBSession session, Model model) throws JasDBException {
        model.addAttribute("instances", session.getInstances());
        model.addAttribute("instanceId", session.getInstanceId());
        model.addAttribute("instance", session.getInstance(session.getInstanceId()));

        //required pre-filled empty forms
        model.addAttribute("bag", new Bag());
        model.addAttribute("entity", new WebEntity());
        model.addAttribute("searchForm", new SearchForm());
        model.addAttribute("instanceForm", new WebInstance());

        return "data/index";
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public String createInstance(@Valid WebInstance instance) throws JasDBException {
        DBSession session = sessionFactory.createSession();
        session.addInstance(instance.getName());

        return "redirect:/data/";
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

    @RequestMapping(value = "/{instanceId}/{bag}/delete", method = RequestMethod.GET)
    public String deleteBag(@PathVariable String instanceId, @PathVariable String bag) throws JasDBException {
        DBSession session = sessionFactory.createSession(instanceId);
        session.removeBag(instanceId, bag);

        return "redirect:/data/";
    }

    @RequestMapping(value = "/{instanceId}/delete", method = RequestMethod.GET)
    public String deleteInstance(@PathVariable String instanceId) throws JasDBException {
        DBSession session = sessionFactory.createSession();
        session.deleteInstance(instanceId);

        return "redirect:/data/";
    }

}
