package com.obera.jasdb.web;

import com.obera.jasdb.web.model.PageResult;
import com.obera.jasdb.web.model.SearchForm;
import com.obera.jasdb.web.model.WebEntity;
import com.oberasoftware.jasdb.core.model.EntityBag;
import com.oberasoftware.jasdb.core.query.QueryBuilder;
import com.oberasoftware.jasdb.core.query.QueryResult;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * @author renarj
 */
@Controller
@RequestMapping(value = "/search")
public class QueryController {
    private static final int DEFAULT_PAGE_SIZE = 20;

    @Autowired
    private DBSessionFactory sessionFactory;

    @RequestMapping(value = "/{instanceId}/{bag}")
    public String findAll(@PathVariable String instanceId, @PathVariable String bag, Model model) throws JasDBException {
        DBSession session = sessionFactory.createSession(instanceId);
        EntityBag entityBag = session.getBag(bag);

        PageResult result = new PageResult();
        model.addAttribute("page", result);

        if(entityBag != null) {
            QueryResult queryResult = entityBag.getEntities(DEFAULT_PAGE_SIZE);

            result.setEntities(loadEntities(queryResult));
        } else {
            result.setMessage(String.format("Unable to load Bag: %s on instance: %s as it does not exist", bag, instanceId));
        }

        return "data/query";
    }

    @RequestMapping(value = "/{instanceId}/{bag}", method = RequestMethod.POST)
    public String searchFieldValue(SearchForm searchForm, @PathVariable String instanceId, @PathVariable String bag, Model model) throws JasDBException {
        DBSession session = sessionFactory.createSession(instanceId);
        EntityBag entityBag = session.getBag(bag);

        PageResult result = new PageResult();
        model.addAttribute("page", result);

        if(entityBag != null) {
            QueryResult queryResult = entityBag.find(QueryBuilder.createBuilder().field(searchForm.getField()).value(searchForm.getValue())).limit(DEFAULT_PAGE_SIZE).execute();

            result.setEntities(loadEntities(queryResult));
        } else {
            result.setMessage(String.format("Unable to load Bag: %s on instance: %s as it does not exist", bag, instanceId));
        }

        return "data/query";
    }

    private List<WebEntity> loadEntities(QueryResult result) throws JasDBException {
        List<WebEntity> entities = new ArrayList<>();
        for(SimpleEntity entity : result) {
            WebEntity webEntity = new WebEntity();
            webEntity.setData(SimpleEntity.toJson(entity));
            webEntity.setId(entity.getInternalId());

            entities.add(webEntity);
        }

        return entities;
    }
}
