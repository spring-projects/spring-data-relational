package org.springframework.data.relational.core.query.template;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Basic Implementation of Dynamic Template SQL Provider
 *
 * @author kfyty725
 * @date 2023/10/21 18:31
 * @email kfyty725@hotmail.com
 */
public abstract class AbstractDynamicTemplateProvider<TS extends TemplateStatement> implements DynamicTemplateProvider<TS>, ResourceLoaderAware, InitializingBean {
    private static final Log LOG = LogFactory.getLog(DynamicTemplateProvider.class);

    /**
     * Resolve the dynamic SQL template set based on the given path
     */
    protected List<String> paths;

    /**
     * Resource scanner
     */
    protected ResourcePatternResolver resourcePatternResolver;

    /**
     * Parsed dynamic template statement
     */
    protected Map<String, TS> templateStatements = new ConcurrentHashMap<>();

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourcePatternResolver = ResourcePatternUtils.getResourcePatternResolver(resourceLoader);
    }

    @Override
    public List<TS> resolve(List<String> paths) {
        try {
            List<TS> templateStatements = new ArrayList<>();
            for (String path : paths) {
                for (Resource resource : this.resourcePatternResolver.getResources(ResourceUtils.CLASSPATH_URL_PREFIX + path)) {
                    Element rootElement = createElement(resource.getInputStream());
                    String namespace = resolveAttribute(rootElement, TemplateStatement.TEMPLATE_NAMESPACE, () -> new IllegalArgumentException("namespace can't empty"));
                    NodeList select = rootElement.getElementsByTagName(TemplateStatement.SELECT_LABEL);
                    NodeList execute = rootElement.getElementsByTagName(TemplateStatement.EXECUTE_LABEL);
                    templateStatements.addAll(this.resolveInternal(namespace, TemplateStatement.SELECT_LABEL, select));
                    templateStatements.addAll(this.resolveInternal(namespace, TemplateStatement.EXECUTE_LABEL, execute));
                    LOG.info("resolved resource: " + resource.getDescription());
                }
            }
            return templateStatements;
        } catch (IOException e) {
            throw new IllegalArgumentException("resolve template failed", e);
        }
    }

    @Override
    public String resolveTemplateStatementId(Method method) {
        String id = method.getDeclaringClass().getName() + "." + method.getName();
        if (!this.templateStatements.containsKey(id)) {
            throw new IllegalArgumentException("template statement not exists of id: " + id);
        }
        return id;
    }

    @Override
    public void afterPropertiesSet() {
        List<TS> resolve = this.resolve(this.paths);
        resolve.forEach(e -> this.templateStatements.put(e.getId(), e));
    }

    /**
     * Internal parsing, implemented by subclasses
     */
    protected abstract List<TS> resolveInternal(String namespace, String labelType, NodeList nodeList);

    /**
     * load xml document from an xml input stream
     */
    private static Element createElement(InputStream inputStream) {
        try {
            return DocumentBuilderFactoryHolder.INSTANCE.newDocumentBuilder().parse(inputStream).getDocumentElement();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            throw new IllegalArgumentException("load dynamic template failed", e);
        }
    }

    /**
     * resolve xml attribute from xml element
     */
    private static String resolveAttribute(Element element, String name, Supplier<RuntimeException> emptyException) {
        String attribute = element.getAttribute(name);
        if (emptyException != null && !StringUtils.hasText(attribute)) {
            throw emptyException.get();
        }
        return attribute;
    }

    private static final class DocumentBuilderFactoryHolder {
        static final DocumentBuilderFactory INSTANCE = DocumentBuilderFactory.newInstance();
    }
}
