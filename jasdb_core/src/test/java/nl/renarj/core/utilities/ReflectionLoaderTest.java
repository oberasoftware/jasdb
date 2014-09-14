/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.core.utilities;

import nl.renarj.jasdb.core.collections.OrderedBalancedTree;
import org.junit.Assert;
import org.junit.Test;

/**
 * User: renarj
 * Date: 3/21/12
 * Time: 10:16 PM
 */
public class ReflectionLoaderTest {
    @Test
    public void testLoadClass() throws Exception {
        OrderedBalancedTree tree = ReflectionLoader.loadClass(OrderedBalancedTree.class, "nl.renarj.jasdb.core.collections.OrderedBalancedTree", new Object[]{});
        Assert.assertNotNull(tree);
    }

    @Test
    public void testLoadPrivateMethod() throws Exception {
        TestClassWithPrivates tc = new TestClassWithPrivates();
        Object result = ReflectionLoader.invokeMethod(tc, "getPrivateMethod", new Class<?>[] {});
        Assert.assertNotNull(result);
        Assert.assertEquals("Expected privateMethodReturn", "privateMethodReturn", result.toString());
    }

    @Test
    public void testLoadPrivateField() throws Exception {
        TestClassWithPrivates tc = new TestClassWithPrivates();
        
        Object result = ReflectionLoader.getField(tc, "privateField");
        Assert.assertNotNull(result);
        Assert.assertEquals("Expected privateValue", "privateValue", result.toString());
    }
    
    private class TestClassWithPrivates {
        private String privateField;
        
        public TestClassWithPrivates() {
            privateField = "privateValue";
        }
        
        private String getPrivateMethod() {
            return "privateMethodReturn";
        }
    }
    
}
