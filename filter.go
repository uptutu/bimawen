package bimawen

import (
	"fmt"
	"regexp"
	"strings"
)

// MessageFilter represents a filter that can be applied to messages
type MessageFilter interface {
	// Match returns true if the message matches the filter criteria
	Match(message *Message) bool
	
	// String returns a string representation of the filter
	String() string
}

// FilterOperator represents logical operators for combining filters
type FilterOperator int

const (
	// FilterAND requires all filters to match
	FilterAND FilterOperator = iota
	// FilterOR requires at least one filter to match
	FilterOR
	// FilterNOT negates the filter result
	FilterNOT
)

// CompositeFilter combines multiple filters with logical operators
type CompositeFilter struct {
	Operator FilterOperator
	Filters  []MessageFilter
}

// NewCompositeFilter creates a new composite filter
func NewCompositeFilter(operator FilterOperator, filters ...MessageFilter) *CompositeFilter {
	return &CompositeFilter{
		Operator: operator,
		Filters:  filters,
	}
}

// Match implements MessageFilter interface
func (cf *CompositeFilter) Match(message *Message) bool {
	if len(cf.Filters) == 0 {
		return true
	}
	
	switch cf.Operator {
	case FilterAND:
		for _, filter := range cf.Filters {
			if !filter.Match(message) {
				return false
			}
		}
		return true
		
	case FilterOR:
		for _, filter := range cf.Filters {
			if filter.Match(message) {
				return true
			}
		}
		return false
		
	case FilterNOT:
		// For NOT operator, we only use the first filter
		if len(cf.Filters) > 0 {
			return !cf.Filters[0].Match(message)
		}
		return true
		
	default:
		return false
	}
}

// String returns string representation
func (cf *CompositeFilter) String() string {
	if len(cf.Filters) == 0 {
		return "EmptyFilter"
	}
	
	var op string
	switch cf.Operator {
	case FilterAND:
		op = "AND"
	case FilterOR:
		op = "OR"
	case FilterNOT:
		op = "NOT"
	default:
		op = "UNKNOWN"
	}
	
	if cf.Operator == FilterNOT && len(cf.Filters) > 0 {
		return "NOT(" + cf.Filters[0].String() + ")"
	}
	
	var parts []string
	for _, filter := range cf.Filters {
		parts = append(parts, filter.String())
	}
	
	return "(" + strings.Join(parts, " "+op+" ") + ")"
}

// HeaderFilter filters messages based on header values
type HeaderFilter struct {
	Key      string
	Value    interface{}
	Operator FilterOperator // Use for exact match, pattern match, etc.
	Pattern  *regexp.Regexp // For regex pattern matching
}

// NewHeaderFilter creates a new header filter for exact match
func NewHeaderFilter(key string, value interface{}) *HeaderFilter {
	return &HeaderFilter{
		Key:      key,
		Value:    value,
		Operator: FilterAND, // Default to exact match
	}
}

// NewHeaderRegexFilter creates a new header filter with regex pattern
func NewHeaderRegexFilter(key, pattern string) (*HeaderFilter, error) {
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	
	return &HeaderFilter{
		Key:      key,
		Pattern:  regex,
		Operator: FilterOR, // Use OR to indicate pattern matching
	}, nil
}

// Match implements MessageFilter interface
func (hf *HeaderFilter) Match(message *Message) bool {
	if message.Headers == nil {
		return false
	}
	
	headerValue, exists := message.Headers[hf.Key]
	if !exists {
		return false
	}
	
	// Pattern matching
	if hf.Pattern != nil {
		headerStr, ok := headerValue.(string)
		if !ok {
			return false
		}
		return hf.Pattern.MatchString(headerStr)
	}
	
	// Exact match
	return headerValue == hf.Value
}

// String returns string representation
func (hf *HeaderFilter) String() string {
	if hf.Pattern != nil {
		return "Header[" + hf.Key + " matches /" + hf.Pattern.String() + "/]"
	}
	return "Header[" + hf.Key + " == " + toString(hf.Value) + "]"
}

// ContentFilter filters messages based on body content
type ContentFilter struct {
	Pattern *regexp.Regexp
	Exact   string
}

// NewContentFilter creates a new content filter for exact string match
func NewContentFilter(exact string) *ContentFilter {
	return &ContentFilter{
		Exact: exact,
	}
}

// NewContentRegexFilter creates a new content filter with regex pattern
func NewContentRegexFilter(pattern string) (*ContentFilter, error) {
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	
	return &ContentFilter{
		Pattern: regex,
	}, nil
}

// Match implements MessageFilter interface
func (cf *ContentFilter) Match(message *Message) bool {
	content := string(message.Body)
	
	if cf.Pattern != nil {
		return cf.Pattern.MatchString(content)
	}
	
	if cf.Exact != "" {
		return strings.Contains(content, cf.Exact)
	}
	
	return true
}

// String returns string representation
func (cf *ContentFilter) String() string {
	if cf.Pattern != nil {
		return "Content[matches /" + cf.Pattern.String() + "/]"
	}
	return "Content[contains '" + cf.Exact + "']"
}

// PriorityFilter filters messages based on priority level
type PriorityFilter struct {
	MinPriority uint8
	MaxPriority uint8
}

// NewPriorityFilter creates a new priority filter
func NewPriorityFilter(minPriority, maxPriority uint8) *PriorityFilter {
	return &PriorityFilter{
		MinPriority: minPriority,
		MaxPriority: maxPriority,
	}
}

// Match implements MessageFilter interface
func (pf *PriorityFilter) Match(message *Message) bool {
	return message.Priority >= pf.MinPriority && message.Priority <= pf.MaxPriority
}

// String returns string representation
func (pf *PriorityFilter) String() string {
	return "Priority[" + toString(pf.MinPriority) + " <= priority <= " + toString(pf.MaxPriority) + "]"
}

// TopicFilter filters messages based on topic patterns
type TopicFilter struct {
	Pattern *regexp.Regexp
	Exact   string
}

// NewTopicFilter creates a new topic filter for exact match
func NewTopicFilter(exact string) *TopicFilter {
	return &TopicFilter{
		Exact: exact,
	}
}

// NewTopicPatternFilter creates a new topic filter with pattern matching
func NewTopicPatternFilter(pattern string) (*TopicFilter, error) {
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	
	return &TopicFilter{
		Pattern: regex,
	}, nil
}

// Match implements MessageFilter interface for topic-based filtering
// Note: This requires topic context which isn't part of Message struct
// This is used by routing logic that has access to topic information
func (tf *TopicFilter) Match(message *Message) bool {
	// For message-only filtering, we can't match topic
	// This filter is used by routing logic with topic context
	return true
}

// MatchTopic matches against a specific topic
func (tf *TopicFilter) MatchTopic(topic string) bool {
	if tf.Pattern != nil {
		return tf.Pattern.MatchString(topic)
	}
	
	if tf.Exact != "" {
		return tf.Exact == topic
	}
	
	return true
}

// String returns string representation
func (tf *TopicFilter) String() string {
	if tf.Pattern != nil {
		return "Topic[matches /" + tf.Pattern.String() + "/]"
	}
	return "Topic[== '" + tf.Exact + "']"
}

// FilterRegistry manages registered message filters
type FilterRegistry struct {
	filters map[string]MessageFilter
}

// NewFilterRegistry creates a new filter registry
func NewFilterRegistry() *FilterRegistry {
	return &FilterRegistry{
		filters: make(map[string]MessageFilter),
	}
}

// Register registers a filter with a name
func (fr *FilterRegistry) Register(name string, filter MessageFilter) {
	fr.filters[name] = filter
}

// Get retrieves a filter by name
func (fr *FilterRegistry) Get(name string) (MessageFilter, bool) {
	filter, exists := fr.filters[name]
	return filter, exists
}

// List returns all registered filter names
func (fr *FilterRegistry) List() []string {
	var names []string
	for name := range fr.filters {
		names = append(names, name)
	}
	return names
}

// Clear removes all registered filters
func (fr *FilterRegistry) Clear() {
	fr.filters = make(map[string]MessageFilter)
}

// Utility function to convert values to string for display
func toString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("%d", v)
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint:
		return fmt.Sprintf("%d", v)
	case uint8:
		return fmt.Sprintf("%d", v)
	case uint16:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%.2f", v)
	case float64:
		return fmt.Sprintf("%.2f", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}