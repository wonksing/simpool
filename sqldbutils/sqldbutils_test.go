package sqldbutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTextDatabaseType(t *testing.T) {
	assert.Equal(t, true, isTextDatabaseType("CHAR"))
	assert.Equal(t, true, isTextDatabaseType("char"))
	assert.Equal(t, true, isTextDatabaseType("cHar"))
	assert.Equal(t, true, isTextDatabaseType("cHar "))

	assert.Equal(t, false, isTextDatabaseType("number"))
}
