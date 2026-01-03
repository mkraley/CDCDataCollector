# Debugging Instructions for Chrome DevTools

## Step 1: Open Chrome DevTools
1. Press `F12` or right-click on the page and select "Inspect"
2. Go to the **Console** tab

## Step 2: Set up Monitoring

### Monitor Property Changes on forge-paginator:
```javascript
// Get the paginator element
fp = document.querySelector('forge-paginator')

// Create a Proxy to monitor property changes
const handler = {
  set(target, property, value) {
    console.log(`Property ${property} changed from ${target[property]} to ${value}`);
    target[property] = value;
    return true;
  }
};
fpMonitored = new Proxy(fp, handler);

// Now manually change the select and watch the console
```

### Monitor Method Calls:
```javascript
fp = document.querySelector('forge-paginator')

// Wrap methods to see when they're called
const originalPageSizeSetter = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(fp), 'pageSize');
if (originalPageSizeSetter && originalPageSizeSetter.set) {
  Object.defineProperty(fp, 'pageSize', {
    set: function(value) {
      console.log('pageSize setter called with:', value);
      originalPageSizeSetter.set.call(this, value);
    },
    get: originalPageSizeSetter.get
  });
}
```

### Monitor Events:
```javascript
// Monitor all events on the paginator
fp = document.querySelector('forge-paginator')
const originalDispatchEvent = fp.dispatchEvent.bind(fp);
fp.dispatchEvent = function(event) {
  console.log('Event dispatched:', event.type, event);
  return originalDispatchEvent(event);
};

// Monitor events on the select
fs = fp.shadowRoot.querySelector('forge-select')
const originalSelectDispatch = fs.dispatchEvent.bind(fs);
fs.dispatchEvent = function(event) {
  console.log('Select event dispatched:', event.type, event);
  return originalSelectDispatch(event);
};
```

## Step 3: Change the Select Manually
1. Click on the "Rows per page" dropdown
2. Select "100"
3. Watch the Console for any logged property changes, method calls, or events

## Step 4: Check What Actually Changed
After changing the select, run:
```javascript
fp = document.querySelector('forge-paginator')
console.log('pageSize:', fp.pageSize);
console.log('pageIndex:', fp.pageIndex);
console.log('offset:', fp.offset);
console.log('total:', fp.total);
```

## Alternative: Use Chrome's Performance Monitor
1. Open DevTools
2. Go to **Performance** tab
3. Click the Record button (circle icon)
4. Change the select dropdown manually
5. Stop recording
6. Look at the timeline to see what functions were called

## Alternative: Use Event Listeners Panel
1. Open DevTools
2. Go to **Elements** tab
3. Find the `forge-paginator` element in the DOM tree
4. In the right panel, look for **Event Listeners**
5. Expand to see what events the element is listening for
6. Change the select and see which listeners fire

