#!/bin/bash
echo "=== Testing Learning Mode Compatibility (Multi-Process) ==="
echo ""

# Test 1: Enable learning on port 5000
echo "1. Enabling learning mode for 'conn' on port 5000..."
RESPONSE1=$(docker exec lstm-autoencoder curl -s -X POST http://localhost:5000/learning/enable/conn)
echo "Response: $RESPONSE1"
echo ""

# Test 2: Check status on port 5001 (different process)
echo "2. Checking learning status for 'conn' on port 5001 (should see enabled)..."
RESPONSE2=$(docker exec lstm-autoencoder curl -s http://localhost:5001/learning/status/conn)
echo "Response: $RESPONSE2"
echo ""

# Test 3: Check status on port 5002 (different process)
echo "3. Checking learning status for 'conn' on port 5002 (should see enabled)..."
RESPONSE3=$(docker exec lstm-autoencoder curl -s http://localhost:5002/learning/status/conn)
echo "Response: $RESPONSE3"
echo ""

# Test 4: Update buffer on port 5000
echo "4. Updating buffer size on port 5000..."
RESPONSE4=$(docker exec lstm-autoencoder curl -s -X POST -H "Content-Type: application/json" -d '{"buffer_size": 123}' http://localhost:5000/buffer/update/conn)
echo "Response: $RESPONSE4"
echo ""

# Test 5: Check buffer on port 5001 (should see the update)
echo "5. Checking buffer size on port 5001 (should see 123)..."
RESPONSE5=$(docker exec lstm-autoencoder curl -s http://localhost:5001/learning/status/conn)
echo "Response: $RESPONSE5"
echo ""

echo "=== Test Complete ==="
echo "If all processes show the same learning status and buffer size, compatibility is working!"
