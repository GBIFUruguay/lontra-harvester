package net.canadensys.harvester.config;

import java.util.regex.Pattern;

import org.springframework.core.type.ClassMetadata;
import org.springframework.core.type.filter.RegexPatternTypeFilter;

/**
 * Used to exclude all files that end with 'Test' (convention over configuration) from net.canadensys.processing package.
 * The goal is to avoid annotation and bean conflicts.
 * 
 * @author canadensys
 *
 */
public class ExcludeTestClassesTypeFilter extends RegexPatternTypeFilter{
    public ExcludeTestClassesTypeFilter() {
        super(Pattern.compile("net\\.canadensys\\.harvester.*Test$"));
    }
    
    @Override
    protected boolean match(ClassMetadata metadata) {
    	return super.match(metadata);
    }
}
